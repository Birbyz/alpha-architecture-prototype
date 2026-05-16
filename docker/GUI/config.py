from dataclasses import dataclass
from pathlib import Path
from common.utils import get_env_var


@dataclass(frozen=True)
class GuiConfig:
    max_log_lines = 2000

    # path to docker-compose.yml
    compose_dir = Path("../docker").resolve()

    # docker-compose file name
    compose_file = "docker-compose.yml"

    # container/service name for spark reads
    spark_client_service: str = "spark-client"

    # gold delta path
    gold_delta_path: str = get_env_var("DELTA_PATH_GOLD")
