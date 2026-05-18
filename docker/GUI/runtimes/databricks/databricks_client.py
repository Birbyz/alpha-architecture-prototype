import requests
import os
import base64
from typing import Optional

from constants import DATABRICKS_RUNNING_STATES, DATABRICKS_TERMINAL_STATES


# maximum byte sent per DBFS add-block call
_DBFS_BLOCK_SIZE = int(os.getenv("DATABRICKS_DBFS_BLOCK_SIZE", "1048576"))

# --------------------------------------------------
# Databricks Jobs API 2.1 wrapper
#
# Life-cycle states:  PENDING | RUNNING | TERMINATING | TERMINATED | SKIPPED | INTERNAL_ERROR
# Result states:      SUCCESS | FAILED  | TIMEDOUT    | CANCELED
# --------------------------------------------------
class DatabricksClient:
    def __init__(self, host: str, token: str, timeout: int = 15):
        self.host = host.rstrip("/")
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        self._timeout = timeout
        
    def _get(self, path: str, **kwargs) -> requests.Response:
        return requests.get(
            f"{self.host}{path}",
            headers=self._headers,
            timeout=self._timeout,
            **kwargs,
        )
            
    def _post(self, path: str, payload: dict) -> requests.Response:
        return requests.post(
            f"{self.host}{path}",
            headers=self._headers,
            json=payload,
            timeout=self._timeout,
        )

    # -------- connectivity --------
    def is_connected(self) -> bool:
        # lists current user via SCIM API
        try:
            response = requests.get(
                f"{self.host}/api/2.0/preview/scim/v2/Me",
                headers=self._headers,
                timeout=self._timeout,
            )
            return response.status_code == 200
        except Exception:
            return False

    # -------- job control --------
    def run_now(self, job_id: int) -> int:
        # trigger a job and return the new job_id
        response = self._post(
            f"/api/2.1/jobs/run-now",
            {"job_id": job_id}
        )
        response.raise_for_status()
        return response.json()["run_id"]

    def cancel_run(self, run_id: int) -> None:
        # cancel an active run
        try:
            response = self._post(
                f"/api/2.1/jobs/runs/cancel",
                {"run_id": run_id}
            )
            response.raise_for_status()
        except Exception:
            pass  # run may have already finished

    # -------- status polling --------
    def get_run_state(self, run_id: int) -> dict:
        """
        Returns the raw state dict from the Runs Get API, e.g.:
            {"life_cycle_state": "RUNNING", "state_message": "..."}
        or on termination:
            {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS", ...}
        """
        response = self._get(
            f"/api/2.1/jobs/runs/get",
            params={"run_id": run_id}
        )
        response.raise_for_status()
        return response.json().get("state", {})

    def is_run_active(self, run_id: int) -> bool:
        # True while the run is PENDING / RUNNING / TERMINATING.
        try:
            state = self.get_run_state(run_id)
            return state.get("life_cycle_state", "") in DATABRICKS_RUNNING_STATES
        except Exception:
            return False

    def get_run_result(self, run_id: int) -> Optional[str]:
        # Returns result_state string once the run is terminal, else None.
        # Possible values: SUCCESS | FAILED | TIMEDOUT | CANCELED
        try:
            state = self.get_run_state(run_id)
            if state.get("life_cycle_state", "") in DATABRICKS_TERMINAL_STATES:
                return state.get("result_state")
        except Exception:
            pass

        return None
    
    # DBFS API
    def dbfs_exists(self, path: str) -> bool:
        try:
            response = self._get(
                f"/api/2.0/dbfs/get-status",
                params={"path": path}
            )
            return response.status_code == 200
        except Exception:
            return False
        
    def dbfs_mkdirs(self, path: str):
        response = self._post(
            f"/api/2.0/dbfs/mkdirs",
            {"path": path}
        )
        response.raise_for_status()
        
    def dbfs_upload(self, local_path: str, dbfs_path: str, overwrite: bool = False):
        if not os.path.isfile(local_path):
            raise FileNotFoundError(f"Local file not found: {local_path}")
        
        # open a DBFS stream handle
        create_response = self._post(
            f"/api/2.0/dbfs/create",
            {"path": dbfs_path, "overwrite": overwrite}
        )
        create_response.raise_for_status()
        handle = create_response.json()["handle"]
        
        try:
            # stream the file in 1MB blocks
            with open(local_path, "rb") as f:
                while True:
                    chunk = f.read(_DBFS_BLOCK_SIZE)
                    if not chunk:
                        break # EOF
                    
                    encoded = base64.b64encode(chunk).decode("utf-8")
                    block_response = self._post(
                        f"/api/2.0/dbfs/add-block",
                        {"handle": handle, "data": encoded}
                    )
                    block_response.raise_for_status()
                    
            # close the handle to finalize the file
            close_response = self._post(
                f"/api/2.0/dbfs/close",
                {"handle": handle}
            )
            close_response.raise_for_status()
            
        except Exception as e:
            try:
                # attempt to close the handle on error to avoid leaks
                self._post(
                    f"/api/2.0/dbfs/close",
                    {"handle": handle}
                )
            except Exception:
                pass
            raise e
        
    def get_cluster_state(self, cluster_id: str) -> Optional[str]:
        if not cluster_id:
            return None
        
        try:
            response = self._get(
                f"/api/2.0/clusters/get",
                params={"cluster_id": cluster_id}
            )
            response.raise_for_status()
            return response.json().get("state")
        except Exception:
            pass
        
        return None

