import os
import sys
import time
import logging.config
import json
from collections import UserDict
from typing import Generator

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from dataclasses import dataclass, fields
import boto3
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

class ServiceDict(UserDict):
    def __init__(self, session):
        super().__init__()
        self.session = session

    def __getattr__(self, attr):
        return self[attr]

    def __missing__(self, key):
        return self.session.client(key)

aws_services = ServiceDict(boto3.Session())

@dataclass
class Board:
    board_id: str
    board_name: str
    is_kanban: bool
    limit: int
    excluded_task_types: list[str]

@dataclass
class Sprint:
    id: int
    self: str
    state: str
    name: str
    startDate: str
    endDate: str
    completeDate: str
    activatedDate: str
    synced: bool
    autoStartStop: bool

def from_dict(cls, data: dict):
    """Initialize a dataclass from a dictionary, ignoring extra fields."""
    field_names = {f.name for f in fields(cls)}
    filtered_data = {k: v for k, v in data.items() if k in field_names}
    return cls(**filtered_data)


class JSMConfig:
    BOARDS_FILE_NAME = "boards.json"

    def __init__(self, secret_name: str, raw_location: str, output_location: str):
        if secret_name.startswith("sds"):
            self._api_key = wr.secretsmanager.get_secret(secret_name)
        else:
            self._api_key = secret_name

        cfg_boards_locations = [
            os.path.abspath(os.path.join(os.path.dirname(__file__), f"../config/{JSMConfig.BOARDS_FILE_NAME}")),
            os.path.join(os.environ.get('EXTRA_FILES_DIR', ''), JSMConfig.BOARDS_FILE_NAME),
        ]
        cfg_board_search_locations = [f for f in cfg_boards_locations if os.path.exists(f)]
        if len(cfg_board_search_locations) == 0:
            raise ValueError(f"Config file not found in locations: {str(cfg_boards_locations)}")

        with open(cfg_board_search_locations[0], "r") as f:
            data = json.loads(f.read())
            self._boards = [Board(**d) for d in data['boards']]
            pass

    @property
    def api_key(self) -> str:
        return self._api_key

    @property
    def boards(self) -> list[Board]:
        return self._boards

class APIGetClient:
    DOMAIN_NAME = "https://jira.sixt.com"

    def __init__(self, api_key: str):
        self._api_key = api_key

    def obtain_session(self) -> requests.Session:
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

    def fetch_paged(self, url: str, page_size: int, params: dict=None) -> Generator[list, None, None]:
        start_at = 0
        api_url = APIGetClient.DOMAIN_NAME + url

        with self.obtain_session() as session:
            while True:
                page_params = {"startAt": start_at, "maxResults": page_size}
                request_params = {**({} if params is None else params), **page_params}

                logger.info(f"Reading from RestAPI with params: {str(request_params)}, url: {api_url}")

                start_time = time.time()
                response = session.get(api_url, params=request_params)
                end_time = time.time()
                logger.info(f"Fetching from Rest API completed in {(end_time - start_time):.2f} seconds")

                # sometimes bad request are returning for logical reasons
                if response.status_code != requests.codes.bad_request:
                    response.raise_for_status()

                try:
                    response_json = response.json()
                except ValueError as e:
                    logger.error(f"Response is not a valid JSON: {e}")
                    raise

                if 'errorMessages' in response_json:
                    logger.error(f"Data fetch errors: {str(response_json['errorMessages'])}")
                    break

                response.raise_for_status()

                if 'values' not in response_json or len(response_json['values']) == 0:
                    logger.info("No data read, exiting")
                    break
                else:
                    response_result = response_json['values']
                    if isinstance(response_result, list):
                        start_at += len(response_result)
                        yield response_result
                    else:
                        logger.error(f"Response result is not a list: {str(response_result)}, full response: {response_json}")
                        raise ValueError(f"Response result is invalid, see log for details")

    def fetch_simple(self, url: str, params: dict=None) -> dict:
        api_url = APIGetClient.DOMAIN_NAME + url

        with self.obtain_session() as session:
            request_params = {} if params is None else params
            logger.info(f"Reading from RestAPI with params: {str(request_params)}, url: {api_url}")

            start_time = time.time()
            response = session.get(api_url, params=request_params)
            end_time = time.time()
            logger.info(f"Fetching from Rest API completed in {(end_time - start_time):.2f} seconds")

            try:
                response_json = response.json()
            except ValueError as e:
                logger.error(f"Response is not a valid JSON: {e}")
                raise

            if 'errorMessages' in response_json:
                logger.error(f"Data fetch errors: {str(response_json['errorMessages'])}")
                return {}

            response.raise_for_status()

            return response_json


class APIDataFetcher:
    def __init__(self, api_key: str):
        self.api_client = APIGetClient(api_key)

    def fetch_sprints_for_board(self, board: Board) -> list:
        logger.info(f"Fetching sprints for board: {board.board_id}")
        sprints = []

        url = f"/rest/agile/1.0/board/{board.board_id}/sprint"
        for board_sprints_data in self.api_client.fetch_paged(url, board.limit):
            sprints.extend(board_sprints_data)
            pass

        logger.info(f"Fetching sprints for board {board.board_id} completed")
        return sprints

    def fetch_board_configuration(self, board: Board) -> tuple[int, list[str]]:
        logger.info(f"Fetching board configuration for board: {board.board_id}")

        url = f"/rest/agile/1.0/board/{board.board_id}/configuration"
        board_configuration = self.api_client.fetch_simple(url)

        filter_id, columns = (board_configuration['filter']['id'],
                              [c['name'] for c in board_configuration['columnConfig']['columns']])
        logger.info(f"Fetching board configuration {board.board_id} completed: filter: {filter_id}, columns: {columns}")

        return filter_id, columns

class APIDataWriter:
    def __init__(self, location: str, folder_name: str):
        self.location = location
        self.folder_name = folder_name
        self.full_location = f"{self.location}{self.folder_name}"

    def prepare(self):
        if self.location.startswith('s3'):
            logger.info(f"Preparing S3 location: {self.full_location}")
            wr.s3.delete_objects(self.full_location)

    def write_board(self, board: Board, data: list) -> None:
        if self.location.startswith('s3'):
            logger.info(f"Writing to S3 location: {self.full_location}: board={board.board_id}")
            bucket = self.location.split('/')[2]
            key = '/'.join(self.location.split('/')[3:]) + f"boards/{board.board_id}.json"
            logger.info(f"Writing to S3 location: {self.location}, bucket={bucket}, key={key}: board={board.board_id}")
            aws_services.s3.put_object(
                Body=json.dumps(data),
                Bucket=bucket,
                Key=key
            )
        else:
            logger.info(f"Writing to local file: {self.location}: board={board.board_id}")
            folder_name = os.path.abspath(os.path.join(os.path.dirname(__file__), f"../data/board_id={board.board_id}"))
            if not os.path.exists(folder_name):
                os.makedirs(folder_name)
            with open(f"{os.path.join(folder_name, board.board_name)}.json", "w") as f:
                f.write(json.dumps(data))

class BoardDataLoader:
    def __init__(self, api_key: str, location: str):
        self.fetcher = APIDataFetcher(api_key)
        self.writer = APIDataWriter(location, "boards")

    def prepare(self):
        self.writer.prepare()

    def load_sprints(self, board: Board) -> None:
        sprints = self.fetcher.fetch_sprints_for_board(board)
        if len(sprints) == 0:
            logger.error(f"No data for board {board.board_id}, skipping")
        else:
            self.writer.write_board(board, sprints)

    def execute(self, board: Board):
        board_filter_id, board_columns = self.fetcher.fetch_board_configuration(board)
        pass

if __name__ == "__main__":
    args = getResolvedOptions(sys.argv,[
        'jsm_secret_name',
        's3_raw_location',
        's3_output_location'
    ])
    jsm_secret_name = args['jsm_secret_name']
    s3_raw_location = args['s3_raw_location']
    s3_output_location = args['s3_output_location']

    config = JSMConfig(jsm_secret_name, s3_raw_location, s3_output_location)

    loader = BoardDataLoader(config.api_key, s3_raw_location)
    loader.prepare()
    for config_board in config.boards:
        loader.execute(config_board)
