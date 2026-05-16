from constants import (
    __DOCKER__,
    __RUNNING__,
    __STOPPED__,
    START_DOCKER_COMMAND,
    STOP_DOCKER_COMMAND,
)
from runtimes.docker.actions import (
    START_DOCKER_ACTION,
    STOP_DOCKER_ACTION,
    core_services,
)
from state import set_runtime_state
from utils.logging_utils import log
from runtimes.docker.pipeline_runtime import run_command, run_pipeline_stage


def get_status() -> str:
    ok, output = run_command("docker compose ps --services --status running")

    if not ok:
        # log("[DEBUG] docker status command failed")
        return __STOPPED__

    running = {line.strip() for line in output.splitlines() if line.strip()}
    # log(f"[DEBUG] docker running services -> {sorted(running)}")

    if not running:
        return __STOPPED__

    return __RUNNING__ if core_services.issubset(running) else __STOPPED__


def on_selected() -> None:
    status = get_status()
    set_runtime_state(__DOCKER__, status)

    if status == __RUNNING__:
        log("[HDMAS] LOCAL environment ready. Docker is running.")
    else:
        log("[HDMAS] LOCAL environment selected. Docker is not running yet.")


def handle_action(action: str):
    commands = {
        START_DOCKER_ACTION: START_DOCKER_COMMAND,
        STOP_DOCKER_ACTION: STOP_DOCKER_COMMAND,
    }

    log(f"[HDMAS DOCKER] - executing {action}...")

    ok, output = run_command(commands[action])
    if output:
        log(output)


def start_pipeline():
    handle_action(START_DOCKER_ACTION)


def stop_pipeline():
    for stage in ("gold", "silver", "bronze", "batch", "kafka"):
        run_pipeline_stage(stage, "stop")

    handle_action(STOP_DOCKER_ACTION)


def start_stage(stage: str):
    run_pipeline_stage(stage, "start")


def stop_stage(stage: str):
    run_pipeline_stage(stage, "stop")
