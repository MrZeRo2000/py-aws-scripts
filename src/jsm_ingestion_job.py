import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import time
import logging.config
import json
from collections import UserDict
from functools import cache
from typing import Generator, Optional

from pydantic import BaseModel, Field

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

class Sprint(BaseModel):
    id: int
    board_id: int
    self: str=None
    state: str=None
    name: str
    start_date: Optional[str] = Field(None, alias="startDate")
    end_date: Optional[str] = Field(None, alias="endDate")
    complete_date: Optional[str] = Field(None, alias="completeDate")
    activated_date: Optional[str] = Field(None, alias="activatedDate")
    synced: bool=None
    autoStartStop: bool=None

class Status(BaseModel):
    id: str
    self: str

class Column(BaseModel):
    name: str
    statuses: list[Status]

class BoardFilter(BaseModel):
    id: str

class ColumnConfiguration(BaseModel):
    columns: list[Column]

class BoardConfiguration(BaseModel):
    filter: BoardFilter
    column_config: ColumnConfiguration=Field(alias="columnConfig")

class HistoryAuthor(BaseModel):
    display_name: Optional[str] = Field(None, alias="displayName")

class HistoryItem(BaseModel):
    field: str=None
    from_string: Optional[str] = Field(None, alias="fromString")
    to_string: Optional[str] = Field(None, alias="toString")
    from_value: Optional[str] = Field(None, alias="from")
    to_value: Optional[str] = Field(None, alias="to")

class History(BaseModel):
    created: str
    author: Optional[HistoryAuthor]=None
    items: list[HistoryItem]=None

class ChangeLog(BaseModel):
    histories: list[History]

class Named(BaseModel):
    name: str

class Valued(BaseModel):
    value: str

class Project(BaseModel):
    key: str

class Priority(BaseModel):
    name: str
    id: str

class Fields(BaseModel):
    summary: str
    story_points: Optional[float]=Field(None, alias="customfield_10002")
    labels: Optional[list[str]]=None
    components: Optional[list[Named]]=None
    issue_type: Optional[Named]=Field(None, alias="issuetype")
    flagged: Optional[bool]=None
    resolution_date: Optional[str]=Field(None, alias="resolutiondate")
    resolution: Optional[Named]=None
    status: Optional[Named]=None
    environment: Optional[Valued]=Field(None, alias="customfield_11115")
    created: Optional[str]=None
    project: Optional[Project]=None
    epic_key: Optional[str]=Field(None, alias="customfield_10005")
    epic_name: Optional[str]=Field(None, alias="customfield_10006")
    priority: Optional[Priority]=None
    fix_versions: Optional[list[Named]]=Field(None, alias="fixVersions")

class Issue(BaseModel):
    id: int
    board_id: int=None
    key: str
    fields: Fields
    change_log: Optional[ChangeLog] = Field(None, alias="changelog")

class WorkflowTransition(BaseModel):
    name: str
    from_date: str
    to_date: Optional[str]

class SprintHistory(BaseModel):
    history: list[str]=[]
    active_sprint: str=None

class JSMConfig:
    BOARDS_FILE_NAME = "boards.json"

    def __init__(self, secret_name: str, raw_location: str, output_location: str):
        if secret_name.startswith("sds"):
            self._api_key = wr.secretsmanager.get_secret(secret_name)
        else:
            self._api_key = secret_name
        self._raw_location = raw_location
        self._output_location = output_location

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

    @property
    def raw_location(self) -> str:
        return self._raw_location

    @property
    def output_location(self) -> str:
        return self._output_location

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

    def fetch_paged(self, url: str, page_size: int, data_key: str, params: dict=None) -> Generator[list, None, None]:
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

                if data_key not in response_json or len(response_json[data_key]) == 0:
                    logger.info("No data read, exiting")
                    break
                else:
                    response_result = response_json[data_key]
                    if isinstance(response_result, list):
                        logger.info(f"Fetched {len(response_result)} rows")
                        start_at += len(response_result)
                        yield response_result
                    else:
                        logger.error(f"Response result is not a list: {str(response_result)}, full response: {response_json}")
                        raise ValueError(f"Response result is invalid, see log for details")

    @cache
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
        for board_sprints_data in self.api_client.fetch_paged(url, board.limit, 'values'):
            sprints.extend(board_sprints_data)
            pass

        logger.info(f"Fetching sprints for board {board.board_id} completed, {len(sprints)} sprints")
        return sprints

    def fetch_issues_for_board(self, board: Board, filter_id: str) -> list:
        logger.info(f"Fetching issues for board: {board.board_id}")
        issues = []

        quoted_board_names = [f'"{b}"' for b in board.excluded_task_types]
        params = {
            'jql': f"filter={filter_id} AND issuetype NOT IN ({','.join(quoted_board_names)}) AND (created >= startOfDay(-365d) OR updated >= startOfDay(-365d))",
            'fields': 'summary,labels,components,issuetype,resolutiondate,resolution,status,customfield_11115,customfield_10005,customfield_10006,customfield_10002,customfield_12401,created,project,priority,fixVersions',
            'expand': 'changelog'
        }

        url = f"/rest/api/2/search"
        for board_issues_data in self.api_client.fetch_paged(url, board.limit, 'issues', params):
            issues.extend(board_issues_data)
            pass

        logger.info(f"Fetching issues for board {board.board_id} completed, {len(issues)} issues")
        return issues

    @cache
    def fetch_issue_by_key(self, key: str) -> any:
        logger.info(f"Fetching issue by key: {key}")

        url = f"/rest/api/2/issue/{key}"
        issue = self.api_client.fetch_simple(url)
        logger.info(f"Fetching issue completed")

        return issue

    def fetch_board_configuration(self, board: Board) -> BoardConfiguration:
        logger.info(f"Fetching board configuration for board: {board.board_id}")

        url = f"/rest/agile/1.0/board/{board.board_id}/configuration"
        board_configuration = self.api_client.fetch_simple(url)
        board_configuration_model = BoardConfiguration.model_validate_json(json.dumps(board_configuration))

        logger.info(f"Fetching board configuration {board.board_id} completed: {board_configuration_model}")
        return board_configuration_model

class APIDataWriter:
    def __init__(self, location: str):
        self.location = location

    def prepare(self):
        if self.location.startswith('s3'):
            logger.info(f"Preparing S3 location: {self.location}")
            wr.s3.delete_objects(self.location)

    def write_entity(self, entity_name: str, entity_id: str, data: list[BaseModel]) -> None:
        logger.info(f"Writing entity: {entity_name}, {entity_id}")
        data_object = {f"{entity_name}s": [d.model_dump() for d in data]}

        if self.location.startswith('s3'):
            logger.info(f"Writing to S3 location: {self.location}: {entity_name}: {entity_id}")
            bucket = self.location.split('/')[2]
            key = '/'.join(self.location.split('/')[3:]) + f"{entity_name}s/{entity_id}.json"
            logger.info(f"Writing to S3 location: {self.location}, bucket={bucket}, key={key}: {entity_name}={entity_id}")
            aws_services.s3.put_object(
                Body=json.dumps(data_object),
                Bucket=bucket,
                Key=key
            )
        else:
            logger.info(f"Writing to local file: {self.location}: {entity_name}={entity_id}")
            folder_name = os.path.abspath(os.path.join(os.path.dirname(__file__), f"../data/{entity_name}s"))
            if not os.path.exists(folder_name):
                os.makedirs(folder_name)
            with open(f"{os.path.join(folder_name, entity_id)}.json", "w") as f:
                f.write(json.dumps(data_object))
        logger.info(f"Writing entity {entity_name} completed.")

    def write_sprints(self, board: Board, data: list[BaseModel]) -> None:
        self.write_entity('sprint', board.board_id, data)

    def write_issues(self, board: Board, data: list[BaseModel]) -> None:
        self.write_entity('issue', board.board_id, data)

class APIDataLoader:
    def __init__(self, jsm_config: JSMConfig):
        self.fetcher = APIDataFetcher(jsm_config.api_key)
        self.writer = APIDataWriter(jsm_config.raw_location)

    def prepare(self):
        self.writer.prepare()

    def load_sprints(self, board: Board) -> None:
        sprints = self.fetcher.fetch_sprints_for_board(board)
        if len(sprints) == 0:
            logger.error(f"No data for board {board.board_id}, skipping")
        else:
            sprints_with_board = [{**s, 'board_id': board.board_id} for s in sprints]
            sprints_model = [Sprint.model_validate_json(json.dumps(d)) for d in sprints_with_board]
            self.writer.write_sprints(board, sprints_model)

    def fetch_epic_name(self, epic_key: str) -> str:
        issue = self.fetcher.fetch_issue_by_key(epic_key)
        issue_model = Issue.model_validate_json(json.dumps(issue))
        return issue_model.fields.epic_name

    def load_issues(self, board: Board, filter_id: str) -> None:
        issues = self.fetcher.fetch_issues_for_board(board, filter_id)
        if len(issues) == 0:
            logger.error(f"No issues for board {board.board_id}, skipping")
        else:
            issues_with_board = [{**s, 'board_id': board.board_id} for s in issues]
            issues_model = [Issue.model_validate_json(json.dumps(d)) for d in issues_with_board]

            logger.info("Loading epic names")
            for issue_model in [m for m in issues_model if m.fields.epic_key is not None]:
                issue_model.fields.epic_name = self.fetch_epic_name(issue_model.fields.epic_key)
            logger.info("Epic names loaded")

            self.writer.write_issues(board, issues_model)


    def execute(self, board: Board):
        self.load_sprints(board)
        board_configuration = self.fetcher.fetch_board_configuration(board)
        self.load_issues(board, board_configuration.filter.id)
        pass

class BoardConfigurationTransformer:
    def __init__(self, board_configuration: BoardConfiguration):
        self.board_configuration = board_configuration

    def transform_columns(self) -> dict:
        return {s.id: c.name for c in self.board_configuration.column_config.columns for s in c.statuses}

@dataclass
class StatusHistory:
    created: str
    from_status: str
    to_status: str

class IssueTransformer:
    def __init__(self, issue: Issue):
        self.issue = issue

    def transform_status_history(self) -> list[WorkflowTransition]:
        status_history = [StatusHistory(h.created, h1.from_string, h1.to_string)
                          for h in self.issue.change_log.histories for h1 in h.items
                          if h1.field == 'status' and h1.from_string != h1.to_string]

        first_status_history = status_history[0]
        status_history.insert(0, StatusHistory(self.issue.fields.created, "", first_status_history.from_status))

        workflow_history = [WorkflowTransition(
            name=s.to_status,
            from_date=s.created,
            to_date=(status_history[i + 1].created if i < len(status_history) - 1 else None))
            for (i, s) in enumerate(status_history)]

        return workflow_history

    def transform_sprints(self) -> SprintHistory:
        all_sprints = [hi.from_string if s == 'from' else hi.to_string
                       for h in self.issue.change_log.histories
                       for hi in h.items if hi.field == 'Sprint'
                       for s in ['from', 'to']]
        cleaned_sprints = [s.strip() for s in all_sprints if s is not None]
        return SprintHistory(
            history=cleaned_sprints,
            active_sprint=cleaned_sprints[-1] if len(cleaned_sprints) > 0 else None)

def execute_loader_for_board(board: Board):
    loader.execute(board)

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

    loader = APIDataLoader(config)
    loader.prepare()

    for config_board in config.boards:
        loader.execute(config_board)

    '''
    with ThreadPoolExecutor(max_workers=4) as executor:
        for config_board in config.boards:
            executor.submit(execute_loader_for_board, config_board)
    '''