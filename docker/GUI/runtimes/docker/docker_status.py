import json
import subprocess

from pathlib import Path
from typing import List, Dict

from constants import COMPOSE_DIR, COMPOSE_FILE


def get_service_status() -> List[Dict[str, str]]:
    """
    Reads `docker compose ps` and returns a list of service status dicts.
    Uses JSON output if supported by your Docker Compose version.
    """

    cmd = ["docker", "compose", "-f", str(COMPOSE_FILE), "ps", "--format", "json"]

    try:
        result = subprocess.run(
            cmd, cwd=str(COMPOSE_DIR), capture_output=True, text=True, check=True
        )
    except FileNotFoundError:
        return [
            {
                "Service": "docker",
                "State": "ERROR",
                "Health": "N/A",
                "Details": f"Docker CLI not found in {COMPOSE_DIR}",
            }
        ]
    except subprocess.CalledProcessError as e:
        return [
            {
                "Service": "compose",
                "State": "ERROR",
                "Health": "N/A",
                "Details": (e.stderr or e.stdout or "docker compose ps failed").strip(),
            }
        ]

    raw = result.stdout.strip()
    if not raw:
        return [
            {
                "Service": "compose",
                "State": "STOPPED",
                "Health": "N/A",
                "Details": "No running services found",
            }
        ]

    parsed_rows = []

    # Case 1: JSON array
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            parsed_rows = parsed
        elif isinstance(parsed, dict):
            parsed_rows = [parsed]
    except json.JSONDecodeError:
        parsed_rows = []

    # Case 2: newline-delimited JSON objects
    if not parsed_rows:
        lines = [line.strip() for line in raw.splitlines() if line.strip()]
        for line in lines:
            try:
                parsed_rows.append(json.loads(line))
            except json.JSONDecodeError:
                return [
                    {
                        "Service": "compose",
                        "State": "ERROR",
                        "Health": "N/A",
                        "Details": f"Could not parse docker compose JSON output: {line}",
                    }
                ]

    rows = []
    for item in parsed_rows:
        rows.append(
            {
                "Service": item.get("Service", item.get("Name", "unknown")),
                "State": item.get("State", "unknown"),
                "Health": item.get("Health", "N/A"),
                "Details": item.get("Status", ""),
            }
        )

    return rows
