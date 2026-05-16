import os
import time
import subprocess
from runtimes.docker.pipeline_runtime import run_pipeline_stage
from runtimes.hadoop.hadoop_client import HadoopClient
from runtimes.hadoop.hadoop_config import HadoopConfig
import streamlit as st

from utils.logging_utils import log
from constants import __RUNNING__, __STOPPED__, HADOOP_RUN_IDS, HADOOP, __IDLE__
from state import set_runtime_state
from utils.pipeline_state import set_all_stages, set_stage_state


# --------------------------------------------------
# Internal helpers
# --------------------------------------------------
def _get_client():
    # returns (HadoopClient and HadoopConfig) or (None, None)
    config = HadoopConfig.from_env()

    if not config.is_configured:
        log(
            "[HDMAS - HADOOP] Not configured. Set HADOOP_RM_HOST, HADOOP_SSH_HOST, HADOOP_SSH_USER in .env"
        )
        return None, None

    return HadoopClient(config), config


def _to_hdfs(local_path: str) -> str:
    # Convert a local path to its HDFS mirror.
    # /data/bronze/delta  ->  hdfs:///data/bronze/delta
    # Already-HDFS paths are returned unchanged.
    # include namenode host so URI has authority
    namenode = os.getenv("HADOOP_NAMENODE", "localhost:9000")
    return f"hdfs://{namenode}{local_path}"


def _setup_hdfs_dirs():
    # create HDFS directories that mirror the local Delta paths defined in .env
    paths = [
        os.getenv("DELTA_PATH_BRONZE", "/data/bronze/delta"),
        os.getenv("CHECKPOINT_PATH_BRONZE", "/data/bronze/checkpoints"),
        os.getenv("DELTA_PATH_SILVER", "/data/silver/delta"),
        os.getenv("CHECKPOINT_PATH_SILVER", "/data/silver/checkpoints"),
        os.getenv("DELTA_PATH_GOLD", "/data/gold/delta"),
        os.getenv("CHECKPOINT_PATH_GOLD", "/data/gold/checkpoints"),
        os.getenv("BATCH_DATA_PATH", "/data/batch"),
    ]

    hadoop_home = os.getenv("HADOOP_HOME", "")
    hdfs_bin = f"{hadoop_home}/bin/hdfs" if hadoop_home else "hdfs"

    for path in paths:
        hdfs_path = _to_hdfs(path)
        try:
            result = subprocess.run(
                [hdfs_bin, "dfs", "-mkdir", "-p", hdfs_path],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                log(f"[HDMAS HADOOP] HDFS directory ready at {hdfs_path}")
            else:
                err = (result.stderr or result.stdout).strip()
                if "exists" not in err.lower():
                    log(f"[HDMAS HADOOP] Could not create {hdfs_path} - [ERR]: {err}")
        except Exception as e:
            log(f"[HDMAS HADOOP] <mkdir> failed for {hdfs_path} - [ERR]: {e}")


def _upload_batch_csv() -> bool:
    # upload local batch CSV to HDFS if it has not been yet uploaded
    config = HadoopConfig.from_env()
    local_csv = config.batch_csv_local_path

    if not local_csv:
        log(
            "[HDMAS HADOOP] HADOOP_BATCH_CSV_PATH not set in .env - Batch stage will be skipped."
        )
        return False

    if not os.path.exists(local_csv):
        log(f"[HDMAS HADOOP] Batch CSV not found locally: {local_csv}")
        return False

    hdfs_batch_dir = _to_hdfs(os.getenv("BATCH_DATA_PATH", "/data/batch"))
    csv_filename = os.path.basename(local_csv)
    hdfs_dest = f"{hdfs_batch_dir}/{csv_filename}"

    hadoop_home = os.getenv("HADOOP_HOME", "")
    hdfs_bin = f"{hadoop_home}/bin/hdfs" if hadoop_home else "hdfs"

    # check if data has already been uploaded
    check = subprocess.run(
        [hdfs_bin, "dfs", "-test", "-e", hdfs_dest],
        capture_output=True,
    )
    if check.returncode == 0:
        log(f"[HDMAS HADOOP] Batch CSV already on HDFS: {hdfs_dest}")
        return True

    # upload data
    log(f"[HDMAS HADOOP] Uploading batch CSV to HDFS: {local_csv} → {hdfs_dest}")
    result = subprocess.run(
        [hdfs_bin, "dfs", "-put", local_csv, hdfs_dest],
        capture_output=True,
        text=True,
    )

    if result.returncode == 0:
        log(f"[HDMAS HADOOP] Batch CSV uploaded successfully → {hdfs_dest}")
        return True
    else:
        err = (result.stderr or result.stdout).strip()
        log(f"[HDMAS HADOOP] Failed to upload batch CSV: {err}")
        return False


def _build_common_zip() -> None:
    # build common.zip from common/ package so YARN workers can import it
    project_root = os.getenv("HDMAS_PROJECT_ROOT", "")
    if not project_root:
        # fall back: derive from this file's location
        project_root = os.path.dirname(
            os.path.dirname(
                os.path.dirname(
                    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                )
            )
        )

    common_dir = os.path.join(project_root, "common")
    common_zip = os.path.join(project_root, "common.zip")

    if not os.path.isdir(common_dir):
        log(f"[HDMAS HADOOP] common/ directory not found at {common_dir}")
        return

    # check if rebuild is needed
    zip_mtime = os.path.getmtime(common_zip) if os.path.exists(common_zip) else 0
    needs_rebuild = any(
        os.path.getmtime(os.path.join(common_dir, f)) > zip_mtime
        for f in os.listdir(common_dir)
        if f.endswith(".py")
    )

    if not needs_rebuild:
        log("[HDMAS HADOOP] common.zip is up to date.")
        return

    result = subprocess.run(
        ["zip", "-r", common_zip, "common/"],
        cwd=project_root,
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        log(f"[HDMAS HADOOP] common.zip built: {common_zip}")
    else:
        log(f"[HDMAS HADOOP] Failed to build common.zip: {result.stderr}")


# --------------------------------------------------
# Public interface
# --------------------------------------------------
def get_status() -> str:
    config = HadoopConfig.from_env()
    if not config.is_configured:
        log(
            "[HDMAS HADOOP] Configuration incomplete. HADOOP_RM_HOST, HADOOP_SSH_HOST and HADOOP_SSH_USER might not be set in .env"
        )
        return __STOPPED__

    try:
        HadoopClient(config).is_connected()
        return __RUNNING__
    except Exception as e:
        log(f"[HDMAS HADOOP] Connection check failed - ERR: {e}")
        return __STOPPED__


def on_selected():
    config = HadoopConfig.from_env()

    if not config.is_configured:
        log(
            "[HDMAS HADOOP] Configuration incomplete - HADOOP_RM_HOST, HADOOP_SSH_HOST and HADOOP_SSH_USER might not be set in .env"
        )
        set_runtime_state(HADOOP, __STOPPED__)
        return

    log(f"YARN RM: {config.rm_base_url} | SSH: {config.ssh_user}@{config.ssh_host}")
    log(f"[HDMAS HADOOP] Press <Start Pipeline> to continue")
    set_runtime_state(HADOOP, __IDLE__)


def start_pipeline():
    _run_cluster_scripts(("start-dfs.sh", "start-yarn.sh"))  # ← boot HDFS + YARN
    time.sleep(5)
    log("[HDMAS HADOOP] Connecting to YARN RM...")

    status = get_status()
    if status != __RUNNING__:
        log(
            "[HDMAS HADOOP] Cannot connect — YARN is not reachable. "
            "Check the error above and verify your .env settings."
        )
        return

    set_runtime_state(HADOOP, __RUNNING__)
    log("[HDMAS HADOOP] Connected. Preparing environment...")

    st.session_state[HADOOP_RUN_IDS] = {}
    set_all_stages(__IDLE__)
    _build_common_zip()
    _setup_hdfs_dirs()
    _upload_batch_csv()

    log(
        "[HDMAS HADOOP] YARN RM Connected. Use the Pipeline Status buttons to start each stage."
    )


def stop_pipeline():
    log("[HDMAS HADOOP] Stopping pipeline...")
    for stage in ("Gold", "Silver", "Bronze", "Batch", "Kafka"):
        stop_stage(stage)
    set_runtime_state(HADOOP, __STOPPED__)


def _run_cluster_scripts(scripts: tuple[str, ...]) -> str:
    hadoop_home = os.getenv("HADOOP_HOME", "")
    if not hadoop_home:
        log("[HDMAS HADOOP] HADOOP_HOME is not set. Cluster scripts unavailable.")
        return

    for script in scripts:
        path = f"{hadoop_home}/sbin/{script}"
        try:
            result = subprocess.run([path], capture_output=True, text=True)
        except Exception as e:
            log(f"[HDMAS HADOOP] {script} could not be invoked: {e}")
            continue

        # do not return error if YARN RM is already running
        combined = (result.stderr or result.stdout or "").strip()
        is_already_running = (
            "is running as process" in combined or "already running" in combined.lower()
        )

        if result.returncode == 0:
            log(f"[HDMAS HADOOP] {script} executed successfully.")
        elif is_already_running:
            log(f"[HDMAS HADOOP] {script}: service is already running.")
        else:
            err = (result.stderr or result.stdout).strip()
            log(f"[HDMAS HADOOP] {script} failed: {err}")


def start_stage(stage: str):
    if stage == "Kafka":
        from runtimes.docker.pipeline_runtime import run_pipeline_stage

        run_pipeline_stage("Kafka", "start")
        return

    client, config = _get_client()
    if client is None:
        return

    # Batch requires a CSV on HDFS — skip gracefully if not configured
    if stage == "Batch" and not config.batch_csv_local_path:
        log("[HDMAS HADOOP] Batch stage skipped — HADOOP_BATCH_CSV_PATH not set.")
        return

    script = config.script_for(stage)
    if not script:
        log(f"[HDMAS HADOOP] No HADOOP_JOB_SCRIPT_{stage.upper()} configured")
        return

    try:
        app_id = client.submit_stage(stage)
        app_ids = dict(st.session_state.get(HADOOP_RUN_IDS, {}))
        app_ids[stage] = app_id
        st.session_state[HADOOP_RUN_IDS] = app_ids
        set_stage_state(stage, __RUNNING__)
        log(f"[HDMAS HADOOP] {stage} submitted -> app_id = {app_id}")
    except Exception as e:
        log(f"[HDMAS HADOOP] Failed to start {stage}: {e}")
        set_stage_state(stage, __STOPPED__)


def stop_stage(stage: str):
    if stage == "Kafka":
        from runtimes.docker.pipeline_runtime import run_pipeline_stage

        run_pipeline_stage("Kafka", "stop")
        return

    client, _ = _get_client()
    if client is None:
        return

    app_ids = st.session_state.get(HADOOP_RUN_IDS, {})
    app_id = app_ids.get(stage)

    if not app_id:
        log(f"[HDMAS HADOOP] No active app_id found for {stage}")
        set_stage_state(stage, __STOPPED__)
        return

    try:
        client.kill_app(app_id)
        log(f"[HDMAS HADOOP] {stage} kill requested (app_id={app_id})")
    except Exception as e:
        log(f"[HDMAS HADOOP] kill failed for {stage} - [ERR]: {e}")
    finally:
        app_ids = dict(st.session_state.get(HADOOP_RUN_IDS, {}))
        app_ids.pop(stage, None)
        st.session_state[HADOOP_RUN_IDS] = app_ids
        set_stage_state(stage, __STOPPED__)


# TERMINAL COMMAND FOR LAYERS STATUSES
#
# curl -s "http://localhost:8088/ws/v1/cluster/apps" | python3 -c "
# import sys, json
# apps = json.load(sys.stdin).get('apps', {}).get('app', [])
# for a in sorted(apps, key=lambda x: x.get('startedTime', 0), reverse=True):
#     print(f\"{a['name']:<55} {a['state']:<12} {a.get('finalStatus','')}\")
# "
