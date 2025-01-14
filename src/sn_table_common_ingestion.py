import datetime
import time
import awswrangler as wr
import boto3
import uuid
import pandas as pd
import requests
import logging.config
import json
from typing import Generator
from requests.auth import HTTPBasicAuth
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


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

global logger

def send_e_mail_notification(sns_email_topic_arn: str, e_mail_addresses: list, message_body: str, table_name: str):
    if not e_mail_addresses:
        return

    sns_client = boto3.client('sns')
    for e_mail in e_mail_addresses:
        logger.info(f"Sending e-mail to {e_mail}")

        message = {
            "body": message_body,
            "subject": "ERROR: API call for table: " + table_name,
            "to": e_mail,
            "topic": "DataShop: ServiceNow API Call",
            "extra": {
                "date": datetime.datetime.now().strftime('%Y.%m.%d %H:%M:%S')
            }
        }

        sns_client.publish(
            TargetArn=sns_email_topic_arn, #type: ignore
            Message=json.dumps(message)
        )

class SNConfig:
    def __init__(self, secret_name: str, table_name: str):
        self.secret = wr.secretsmanager.get_secret_json(secret_name)
        self.table_name = table_name

    @property
    def url(self):
        return self.secret["url"].format(self.table_name)

    @property
    def user_name(self):
        return self.secret["user_name"]

    @property
    def password(self):
        return self.secret["password"]

    @property
    def error_notification_list(self):
        return self.secret["error_notification_list"]


class SNReader:
    def __init__(self, endpoint_url: str, user_name: str, password: str, endpoint_default_params: dict, endpoint_rows_limit: int):
        self.endpoint_url = endpoint_url
        self._auth = HTTPBasicAuth(user_name, password)
        self._endpoint_default_params = endpoint_default_params
        self._endpoint_rows_limit = endpoint_rows_limit

    def get_session(self) -> requests.Session:
        s = requests.Session()
        s.auth = self._auth

        retries = Retry(
            total=10,
            backoff_factor=0.5,
        )
        s.mount('https://', HTTPAdapter(max_retries=retries))

        return s

    def read(self) -> Generator[list, None, None]:
        with SNReader.get_session(self) as session:
            offset = 0
            while True:
                cycle_params = {
                    "sysparm_limit": self._endpoint_rows_limit,
                    "sysparm_offset": offset
                }
                request_params = {**self._endpoint_default_params, **cycle_params}

                logger.info(f"Reading from ServiceNow {self.endpoint_url} with params: {request_params}")
                start_time = time.time()
                response = session.get(self.endpoint_url, params=request_params)
                end_time = time.time()
                logger.info(f"Reading from ServiceNow completed in {(end_time - start_time):.2f} seconds")

                response.raise_for_status()

                try:
                    response_json = response.json()
                except ValueError as e:
                    logger.error(f"Response is not a valid JSON: {e}")
                    raise

                if 'error' in response_json:
                    logger.error(f"Error in data response: {response_json['error']}")
                    raise ValueError(f"Error in data response from ServiceNow, see log for details")
                if 'result' not in response_json or len(response_json['result']) == 0:
                    logger.info("No data read, exiting")
                    break
                else:
                    response_result = response_json['result']
                    if isinstance(response_result, list):
                        offset += len(response_result)
                        yield response_result
                    else:
                        logger.error(f"Response result is not a list: {str(response_result)}, full response: {response_json}")
                        raise ValueError(f"Response result is invalid, see log for details")


class SNWriter:
    def __init__(self, bucket: str):
        self._bucket = bucket

    def prepare(self):
        wr.s3.delete_objects(self._bucket)

    def write_to_bucket(self, df: pd.DataFrame):
        file_name = f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S_%f')}_{uuid.uuid4()}"
        path = f"{self._bucket.rstrip('/')}/{file_name}"
        logger.info(f"Writing to {path}")
        wr.s3.to_parquet(df=df, path=path, index=False)
        logger.info("Written")
