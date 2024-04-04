import os
import json
import boto3
import argparse
import datetime
from dateutil.tz import tzutc
from logger import get_logger

logger = get_logger(__name__)


class Config:
    INPUT_BUCKET = 'sds-{}-ingest-external-sftp-files'
    OUTPUT_BUCKET = 'sds-{}-ingest-external-sftp-files-out'

    def __init__(self, env):
        self.env = env
        self.session = boto3.session.Session(profile_name=env)
        self._s3 = self.session.client('s3')
        self._data_path = os.path.join(os.path.dirname(__file__), "../data/")

    def __repr__(self):
        return f"(env: {self.env}, data_path: {self._data_path})"

    @property
    def input_bucket(self):
        return self.INPUT_BUCKET.format(self.env)

    @property
    def data_path(self):
        return self._data_path

    @property
    def s3(self):
        return self._s3


class S3ToEvent:
    def __init__(self, config: Config):
        self.config = config

    def list_files(self) -> list:
        bucket = self.config.input_bucket
        prefix = 'adyen-sftp-data/received_payments_report'

        paginator = self.config.s3.get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': bucket, 'Prefix': prefix}

        result = []
        for page in paginator.paginate(**operation_parameters):
            result.extend(
                [p for p in page['Contents']
                 if p['LastModified'] >= datetime.datetime(2023, 11, 1, tzinfo=tzutc())
                 and p['Size'] > 0
                 ]
            )
        return result

    @staticmethod
    def transform_to_event(ls: list) -> dict:
        return {
            'Records': [
                {'s3': {'bucket': {'name': app_config.input_bucket}, 'object': {'key': list_item['Key'], 'size': list_item['Size']}}}
                for list_item in ls]
        }


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('env')
    args = parser.parse_args()

    app_config = Config(args.env)
    logger.info(f"App config: {repr(app_config)}")

    se = S3ToEvent(app_config)
    files = se.list_files()
    logger.info(f"Collected {len(files)} files")

    event = S3ToEvent.transform_to_event(files)
    logger.info(f"Generated event")

    event_file_name = os.path.join(app_config.data_path, f's3_event_{args.env}.json')
    with open(event_file_name, 'w') as f:
        f.write(json.dumps(event))
    logger.info(f"Event saved to {event_file_name}")
