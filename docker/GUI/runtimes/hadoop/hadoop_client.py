"""
HadoopClient
============
Two transport layers:

1. YARN ResourceManager REST API (no extra deps, just `requests`)
   - is_connected()       → GET /ws/v1/cluster/info
   - get_app_state()      → GET /ws/v1/cluster/apps/{appId}
   - kill_app()           → PUT /ws/v1/cluster/apps/{appId}/state

2. SSH via paramiko (pip install paramiko)
   - submit_stage()       → spark-submit --master yarn ... script.py
                            parses "Submitted application application_XXXXX" from stdout
                            returns the YARN application ID string

YARN application life-cycle states
-----------------------------------
  NEW | NEW_SAVING | SUBMITTED | ACCEPTED | RUNNING | FINISHED | FAILED | KILLED

Final states: FINISHED, FAILED, KILLED
"""

import re
import os
import requests
import paramiko

from typing import Optional
from constants import (
    DELTA_PKG,
    HADOOP_ACTIVE_STATES,
    HADOOP_SUCCESS_FINAL,
    HADOOP_TERMINAL_STATES,
    KAFKA_PKGS,
    KAFKA_STAGES,
    HADOOP_NON_PATH_ENV_VARS,
    HADOOP_PATH_ENV_VARS,
)
from runtimes.hadoop.hadoop_config import HadoopConfig

# Regex to extract app ID from spark-submit output
_APP_ID_RE = re.compile(r"(application_\d+_\d+)")


def _to_hdfs(local_path: str) -> str:
    namenode = os.getenv("HADOOP_NAMENODE", "localhost:9000")
    return f"hdfs://{namenode}{local_path}"


class HadoopClient:
    def __init__(self, config: HadoopConfig, timeout: int = 10):
        self.config = config
        self._timeout = timeout

    # --------------------------------------------------
    # Connectivity
    # --------------------------------------------------
    def is_connected(self) -> bool:
        # Ping the YARN ResourceManager. Returns True on HTTP 200
        # raises ERROR on exception
        url = f"{self.config.rm_base_url}/ws/v1/cluster/info"
        try:
            response = requests.get(url, timeout=self._timeout)
            if response.status_code != 200:
                raise RuntimeError(
                    f"YARN RM returned HTTP {response.status_code}. Expected 200 at {url}"
                )

            return True

        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(
                f"Cannot reach YARN at {url}. Check HADOOP_RM_HOST or HADOOP_RM_PORT"
            ) from e

        except requests.exceptions.Timeout as e:
            raise TimeoutError(
                f"YARN RM at {url} didn't responde within {self._timeout}(s)"
            ) from e

        except (RuntimeError, ConnectionError, TimeoutError):
            raise

        except Exception as e:
            raise RuntimeError(
                f"Unexpected error reaching YARN RM at {url} - [ERR]: {e}"
            ) from e

    # --------------------------------------------------
    # Job submission (SSH)
    # --------------------------------------------------
    def submit_stage(self, stage: str) -> str:
        # SSH into the edge node and run spark-submit for the given stage.
        # Returns the YARN application ID. Raises RuntimeError on failure.
        script = self.config.script_for(stage)
        if not script:
            raise RuntimeError(f"No job script configured for stage '{stage}'.")

        cmd = self._build_submit_cmd(stage, script)

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            connect_kwargs: dict = dict(
                hostname=self.config.ssh_host,
                username=self.config.ssh_user,
                timeout=self.config.ssh_connect_timeout,
            )
            if self.config.ssh_key_path:
                connect_kwargs["key_filename"] = self.config.ssh_key_path
            elif self.config.ssh_password:
                connect_kwargs["password"] = self.config.ssh_password

            ssh.connect(**connect_kwargs)
            _stdin, stdout, stderr = ssh.exec_command(cmd)
            exit_code = stdout.channel.recv_exit_status()

            out = stdout.read().decode("utf-8", errors="replace")
            err = stderr.read().decode("utf-8", errors="replace")
            combined = out + "\n" + err

        finally:
            ssh.close()

        # parse the YARN application ID
        match = _APP_ID_RE.search(combined)
        if not match:
            if exit_code != 0:
                raise RuntimeError(
                    f"spark-submit failed (exit {exit_code}):\n{combined}"
                )
            raise RuntimeError(
                f"spark-submit succeeded but no application ID found in output:\n{combined[:400]}"
            )
        return match.group(1)

    def _build_submit_cmd(self, stage: str, script: str) -> str:
        # build the spark-submit shell command executed over SSH
        app_name = f"hdmas-{stage.lower()}"

        # packages - all stages require Delta
        packages = DELTA_PKG
        if stage in KAFKA_STAGES:
            packages = f"{DELTA_PKG},{KAFKA_PKGS}"

        project_root = os.getenv("HDMAS_PROJECT_ROOT", "").strip() or (
            os.path.dirname(os.path.dirname(os.path.dirname(script)))
        )
        common_zip = os.path.join(project_root, "common.zip")

        py_files_arg = (
            f" --py-files file://{common_zip}" if os.path.isfile(common_zip) else ""
        )
        hadoop_kafka_override = os.getenv("HADOOP_KAFKA_BOOTSTRAP_SERVERS", "").strip()

        # build --conf spark.yarn.appMasterEnv.* flags
        env_confs: list[str] = []
        for var in HADOOP_PATH_ENV_VARS:
            val = os.getenv(var, "").strip()
            if val:
                env_confs.append(
                    f'--conf "spark.yarn.appMasterEnv.{var}={_to_hdfs(val)}"'
                )
        for var in HADOOP_NON_PATH_ENV_VARS:
            val = os.getenv(var, "").strip()
            if var == "KAFKA_BOOTSTRAP_SERVERS" and hadoop_kafka_override:
                val = hadoop_kafka_override
            if val:
                env_confs.append(f'--conf "spark.yarn.appMasterEnv.{var}={val}"')
        env_conf_str = " \\\n  ".join(env_confs)

        exports: list[str] = []
        conf_dir = self.config.hadoop_conf_dir
        if conf_dir:
            exports.append(f"export HADOOP_CONF_DIR={conf_dir}")
            exports.append(f"export YARN_CONF_DIR={conf_dir}")
        for var in HADOOP_PATH_ENV_VARS:
            val = os.getenv(var, "").strip()
            if val:
                exports.append(
                    f"export {var}='{val.replace(chr(39), chr(39)+chr(92)+chr(39)+chr(39))}'"
                )
        for var in HADOOP_NON_PATH_ENV_VARS:
            val = os.getenv(var, "").strip()
            if val:
                exports.append(
                    f"export {var}='{val.replace(chr(39), chr(39)+chr(92)+chr(39)+chr(39))}'"
                )
        if hadoop_kafka_override:
            exports.append(f"export KAFKA_BOOTSTRAP_SERVERS='{hadoop_kafka_override}'")
        env_prefix = " && ".join(exports) + " && " if exports else ""

        return (
            f"{env_prefix}"
            f"{self.config.spark_home}/bin/spark-submit"
            f" --master {self.config.spark_master}"
            f" --deploy-mode {self.config.deploy_mode}"
            f" --name {app_name}"
            f" --driver-memory {os.getenv('HADOOP_DRIVER_MEMORY', '1g')}"
            f" --executor-memory {os.getenv('HADOOP_EXECUTOR_MEMORY', '1g')}"
            f" --packages {packages}"
            f"{py_files_arg}"
            f" --conf spark.yarn.submit.waitAppCompletion=false"
            f" --conf spark.jars.ivy=/tmp/.ivy2"
            f" \\\n  {env_conf_str}"
            f" \\\n  {script}"
            f" 2>&1"
        )

    # --------------------------------------------------
    # Status polling (YARN REST API)
    # --------------------------------------------------
    def get_app_state(self, app_id: str) -> dict:
        resp = requests.get(
            f"{self.config.rm_base_url}/ws/v1/cluster/apps/{app_id}",
            timeout=self._timeout,
        )
        resp.raise_for_status()
        return resp.json().get("app", {})

    def is_app_active(self, app_id: str) -> bool:
        # True while YARN app is still in non-terminal state
        try:
            info = self.get_app_state(app_id)
            return info.get("state", "").upper() in HADOOP_ACTIVE_STATES
        except Exception:
            return False

    def get_app_result(self, app_id: str) -> Optional[str]:
        """
        Returns "SUCCESS", "FAILED", or "KILLED" once the app is terminal.
        Returns None if still running or on error.
        """
        try:
            info = self.get_app_state(app_id)
            state = info.get("state", "").upper()

            if state not in HADOOP_TERMINAL_STATES:
                return None

            final = info.get("finalStatus", "").upper()

            if state == "FINISHED" and final == HADOOP_SUCCESS_FINAL:
                return "SUCCESS"
            if state == "KILLED":
                return "KILLED"
            return "FAILED"
        except Exception:
            return None

    # --------------------------------------------------
    # Job cancellation (YARN REST API)
    # --------------------------------------------------
    def kill_app(self, app_id: str) -> None:
        # Send a KILL signal to a running YARN application
        try:
            response = requests.put(
                f"{self.config.rm_base_url}/ws/v1/cluster/apps/{app_id}/state",
                json={"state": "KILLED"},
                headers={"Content-Type": "application/json"},
                timeout=self._timeout,
            )
            # acceptable response statuses: 200 & 202
            if response.status_code not in (200, 202):
                response.raise_for_status()
        except Exception:
            pass  # app may have already finished
