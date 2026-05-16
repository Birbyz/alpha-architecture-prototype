import json
import time
import threading
import subprocess

from pathlib import Path
from dataclasses import dataclass
from config import GuiConfig
from typing import List, Optional, Tuple


@dataclass
class ServiceStatus:
    name: str
    state: str
    health: str
    details: str


class PipelineManager:
    def __init__(self, cfg: GuiConfig):
        self.cfg = cfg
        self._log_thread: Optional[threading.Thread] = None
        self._log_proc: Optional[subprocess.Popen] = None
        self._log_lock = threading.Lock()
        self._log_lines: List[str] = []
        self._stop_log = threading.Event()

    # -------- docker compose helpers --------
    def _compose_cmd(self, *args: str) -> List[str]:
        return [
            "docker",
            "compose",
            "-f",
            str(self.cfg.compose_dir / self.cfg.compose_file),
            *args,
        ]

    def _run(self, *args: str, timeout: Optional[int] = None) -> Tuple[int, str, str]:
        proc = subprocess.run(
            self._compose_cmd(*args),
            cwd=str(self.cfg.compose_dir),
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return proc.returncode, proc.stdout, proc.stderr

    # -------- public actions --------
    def start_pipeline(self) -> None:
        code, out, err = self._run("up", "-d", "--remove-orphans")
        if code != 0:
            raise RuntimeError(f"docker compose up failed:\n{err or out}")

        # start log streaming
        self.start_log_stream()
        self.wait_until_running(timeout_s=90)

    def stop_pipeline(self) -> None:
        self.stop_log_stream()
        code, out, err = self._run("down")
        if code != 0:
            raise RuntimeError(f"docker compose down failed:\n{err or out}")

    # -------- status / probes --------
    def list_services_status(self) -> List[ServiceStatus]:
        # uses docker compose ps --format json
        code, out, err = self._run("ps", "--format", "json")
        if code != 0:
            # fallback - return a minimal status
            return [
                ServiceStatus(
                    name="compose",
                    state="UNKNOWN",
                    health="UNKNOWN",
                    details=(err or out).strip(),
                )
            ]

        try:
            rows = json.loads(out)
        except Exception:
            return [
                ServiceStatus(
                    name="compose",
                    state="UNKNOWN",
                    health="UNKNOWN",
                    details="Failed to parse JSON output",
                )
            ]

        statuses: List[ServiceStatus] = []
        for r in rows:
            name = r.get("Service", r.get("Name", "unknown"))
            state = r.get("State", "unknown")
            health = r.get("Health", "n/a")
            details = r.get("Status", "")

            statuses.append(
                ServiceStatus(name=name, state=state, health=health, details=details)
            )

        return statuses

    def wait_until_running(self, timeout_s: int = 90) -> None:
        deadline = time.time() + timeout_s

        while time.time() < deadline:
            statuses = self.list_services_status()
            # Consider "running" if every service is running
            if statuses and all(
                s.state.lower() == "running" for s in statuses if s.name != "compose"
            ):
                return
            time.sleep(1.5)

        raise RuntimeError("Timeout waiting for all services to be running.")

    # -------- log streaming --------
    def start_log_stream(self) -> None:
        if self._log_thread and self._log_thread.is_alive():
            return

        self._stop_log.clear()
        cmd = self._compose_cmd("logs", "-f", "--tail", "200")
        self._log_proc = subprocess.Popen(
            cmd,
            cwd=str(self.cfg.compose_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        def _reader():
            assert self._log_proc and self._log_proc.stdout
            for line in self._log_proc.stdout:
                if self._stop_log.is_set():
                    break

                line = line.rstrip("\n")
                with self._log_lock:
                    self._log_lines.append(line)
                    if len(self._log_lines) > self.cfg.max_log_lines:
                        self._log_lines = self._log_lines[-self.cfg.max_log_lines :]

            # drain end

        self._log_thread = threading.Thread(target=_reader, daemon=True)
        self._log_thread.start()

    def stop_log_stream(self) -> None:
        self._stop_log.set()
        if self._log_proc and self._log_proc.poll() is None:
            try:
                self._log_proc.terminate()
            except Exception:
                pass

        self._log_proc = None
        self._log_thread = None

    def get_logs_text(self, last_n: int = 400) -> str:
        with self._log_lock:
            chunk = self._log_lines[-last_n:]

        return "\n".join(chunk)

    # -------- gold sampling via spark --------
    def get_gold_latest_json(self, limit: int = 50) -> List[dict]:
        # Runs a spark-submit inside spark-client to read gold delta and prints JSON lines.
        script_path = Path(__file__).parent

        # pass script inline by cat -> py for portability
        script = script_path.read_text(encoding="utf-8")

        """
            run python with pyspark inside spark-client
            if it FAILS, switch to spark-submit method
        """
        cmd = [
            "docker",
            "compose",
            "-f",
            str(self.cfg.compose_dir / self.cfg.compose_file),
            "exec",
            "-T",
            self.cfg.spark_client_service,
            "bash",
            "-lc",
            f"python - <<'PY'\n{script}\nPY\n",
        ]

        env = {"GOLD_DELTA_PATH": self.cfg.gold_delta_path, "LIMIT": str(limit)}

        proc = subprocess.run(
            cmd,
            cwd=str(self.cfg.compose_dir),
            capture_output=True,
            text=True,
            env={**env, **dict(**{})},
        )

        if proc.returncode != 0:
            raise RuntimeError(
                f"Failed to read gold latest:\n{proc.stderr or proc.stdout}"
            )

        rows: List[dict] = []
        for line in proc.stdout.splitlines():
            line = line.strip()
            if not line:
                continue

            # allow non-json noise lines
            if not (line.startswith("{") and line.endswith("}")):
                continue

            try:
                rows.append(json.loads(line))
            except Exception:
                continue

        return rows
