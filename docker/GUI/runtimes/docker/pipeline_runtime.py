import time
import subprocess
import streamlit as st

from typing import Union
from pathlib import Path
from constants import __RUNNING__, __STOPPED__, COMPOSE_DIR, __IDLE__
from utils.pipeline_state import set_stage_state
from utils.logging_utils import log
from runtimes.docker.actions import (
    BATCH_START_COMMAND,
    BATCH_STOP_COMMAND,
    BRONZE_START_COMMAND,
    BRONZE_STOP_COMMAND,
    GOLD_START_COMMAND,
    GOLD_STOP_COMMAND,
    KAFKA_START_COMMAND,
    KAFKA_STOP_COMMAND,
    SILVER_START_COMMAND,
    SILVER_STOP_COMMAND,
    PIPELINE_STAGE_NAMES,
)


def run_command(cmd: Union[str, list[str]]) -> tuple[bool, str]:
    try:
        if isinstance(cmd, str):
            result = subprocess.run(
                cmd,
                cwd=str(COMPOSE_DIR),
                capture_output=True,
                text=True,
                shell=True,
                check=False,
            )
        else:
            result = subprocess.run(
                cmd,
                cwd=str(COMPOSE_DIR),
                capture_output=True,
                text=True,
                check=False,
            )

        output = (result.stdout or "").strip()
        error = (result.stderr or "").strip()
        msg = "\n".join(part for part in [output, error] if part)

        return result.returncode == 0, msg

    except Exception as e:
        return False, str(e)


def get_stage_command(stage: str, verb: str) -> str:
    command_map = {
        "kafka": {"start": KAFKA_START_COMMAND, "stop": KAFKA_STOP_COMMAND},
        "batch": {"start": BATCH_START_COMMAND, "stop": BATCH_STOP_COMMAND},
        "bronze": {"start": BRONZE_START_COMMAND, "stop": BRONZE_STOP_COMMAND},
        "silver": {"start": SILVER_START_COMMAND, "stop": SILVER_STOP_COMMAND},
        "gold": {"start": GOLD_START_COMMAND, "stop": GOLD_STOP_COMMAND},
    }

    if stage not in command_map:
        return ""

    return command_map[stage].get(verb, "")


def run_pipeline_stage(stage: str, verb: str):
    stage_key = stage.lower()
    ui_stage = PIPELINE_STAGE_NAMES.get(stage_key)

    if not ui_stage:
        log(f"[HDMAS] Unknown pipeline stage: {stage}")
        return

    if verb not in {"start", "stop"}:
        log(f"[HDMAS] Unsupported verb for stage {stage}: {verb}")
        return

    log(f"[HDMAS PIPELINE] {verb.upper()} -> {ui_stage}")

    cmd = get_stage_command(stage_key, verb)
    if not cmd:
        log(f"[HDMAS PIPELINE] No {verb} command configured for {ui_stage}.")
        set_stage_state(ui_stage, __STOPPED__ if verb == "stop" else __IDLE__)
        return

    if verb == "start":
        if "spark-submit" in cmd:
            ok, output = run_background_command(cmd)
        else:
            ok, output = run_command(cmd)

        if output:
            log(output)

        if not ok:
            log(f"[HDMAS PIPELINE] START failed for {ui_stage}")
            set_stage_state(ui_stage, __STOPPED__)
        else:
            # mark as RUNNING and record start time
            # this way, the monitor's grace period doesn't immediately overwrite the status
            set_stage_state(ui_stage, __RUNNING__)
            stage_start_times = st.session_state.get("stage_start_times", {})
            stage_start_times[ui_stage] = time.time()
            st.session_state.stage_start_times = stage_start_times
    else:
        ok, output = run_command(cmd)
        if output:
            log(output)
        set_stage_state(ui_stage, __STOPPED__ if ok else __STOPPED__)


def run_background_command(cmd: str) -> tuple[bool, str]:
    try:
        log_path = Path("/tmp") / f"spark_{int(time.time())}.log"
        with open(log_path, "w") as f:
            subprocess.Popen(
                cmd,
                cwd=str(COMPOSE_DIR),
                shell=True,
                stdout=f,
                stderr=f,
            )
        return True, f"Started in background. Logs: {log_path}"
    except Exception as e:
        return False, str(e)
