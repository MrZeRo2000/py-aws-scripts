import os
import sys
import logging.config
import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from dataclasses import dataclass
import awswrangler as wr
from awsglue.utils import getResolvedOptions


def get_logger(name: str) -> logging.Logger:
    # Logging
    LOGGING_CONFIG = {
        "version": 1,
        "formatters": {
            "human": {
                "class": "logging.Formatter",
                "format": "%(asctime)s:[%(levelname)s:%(lineno)d]: %(message)s"
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "INFO",
                "formatter": "human"
            }
        },
        "root": {
            "level": "INFO",
            "handlers": ["console"]
        },
    }

    # Load logging config
    logging.config.dictConfig(LOGGING_CONFIG)

    return logging.getLogger(name)

logger = get_logger(__name__)

@dataclass
class Board:
    board_id: str
    board_name: str
    is_kanban: bool
    limit: int
    excluded_task_types: list[str]

@dataclass
class BoardsConfig:
    boards: list[Board]

class JSMConfig:
    BOARDS_FILE_NAME = "boards.json"

    def __init__(self, secret_name: str):
        if secret_name.startswith("sds"):
            self._api_key = wr.secretsmanager.get_secret(secret_name)
        else:
            self._api_key = secret_name

        cfg_boards_locations = [os.path.abspath(os.path.join(os.path.dirname(__file__), f))
                                for f in [
                                    JSMConfig.BOARDS_FILE_NAME,
                                    f"../config/{JSMConfig.BOARDS_FILE_NAME}"
                                ]]
        cfg_board_search_locations = [f for f in cfg_boards_locations if os.path.exists(f)]
        if len(cfg_board_search_locations) == 0:
            raise ValueError(f"Config file not found in locations: {str(cfg_boards_locations)}")

        with open(cfg_board_search_locations[0], "r") as f:
            data = json.loads(f.read())
            self._boards = BoardsConfig(**data).boards
            pass

    @property
    def api_key(self) -> str:
        return self._api_key

    @property
    def boards(self) -> list[Board]:
        return self._boards


class APIDataLoader:
    def __init__(self, api_key: str):
        self._api_key = api_key

    def get_session(self) -> requests.Session:
        session = requests.Session()

        session.headers.update({
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json"
        })

        retries = Retry(
            total=10,
            backoff_factor=0.5,
        )
        session.mount('https://', HTTPAdapter(max_retries=retries))

        return session


if __name__ == "__main__":
    args = getResolvedOptions(sys.argv,[
        'jsm_secret_name',
        's3_raw_location',
        's3_output_location'
    ])
    jsm_secret_name = args['jsm_secret_name']
    s3_raw_location = args['s3_raw_location']
    s3_output_location = args['s3_output_location']

    config = JSMConfig(jsm_secret_name)
