import requests

from typing import Optional

from constants import DATABRICKS_RUNNING_STATES, DATABRICKS_TERMINAL_STATES


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

    # -------- connectivity --------
    def is_connected(self) -> bool:
        """Lightweight ping — lists current user via SCIM API."""
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
        # trigger a job and return the new run_id
        response = requests.post(
            f"{self.host}/api/2.1/jobs/run-now",
            headers=self._headers,
            json={"job_id": job_id},
            timeout=self._timeout,
        )
        response.raise_for_status()
        return response.json()["run_id"]

    def cancel_run(self, run_id: int) -> None:
        # cancel an active run
        try:
            response = requests.post(
                f"{self.host}/api/2.1/jobs/runs/cancel",
                headers=self._headers,
                json={"run_id": run_id},
                timeout=self._timeout,
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
        response = requests.get(
            f"{self.host}/api/2.1/jobs/runs/get",
            headers=self._headers,
            params={"run_id": run_id},
            timeout=self._timeout,
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
        """
        Returns result_state string once the run is terminal, else None.
        Possible values: SUCCESS | FAILED | TIMEDOUT | CANCELED
        """
        try:
            state = self.get_run_state(run_id)
            if state.get("life_cycle_state", "") in DATABRICKS_TERMINAL_STATES:
                return state.get("result_state")
        except Exception:
            pass

        return None
