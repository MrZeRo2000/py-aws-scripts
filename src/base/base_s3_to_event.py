import os
import json
import argparse
from abc import ABC

import boto3

from .cfg import BaseConfig, BaseParams
from .logger import get_logger

logger = get_logger(__name__)


class S3ToEventConfig(BaseConfig):
    def __init__(self, env):
        super().__init__(env)
        self._lambda = self.session.client('lambda', region_name=S3ToEventConfig.REGION)

    @property
    def lam(self):
        return self._lambda


class S3ToEventParams(BaseParams):
    def __init__(self, env: str, input_bucket: str, output_bucket: str, prefix: str, limit: int):
        super().__init__(env)
        self._input_bucket = input_bucket
        self._output_bucket = output_bucket
        self._prefix = prefix
        self._limit = limit

    @property
    def input_bucket(self):
        return self._input_bucket.format(self.env)

    @property
    def output_bucket(self):
        return self._output_bucket.format(self.env)

    @property
    def prefix(self):
        return self._prefix

    @property
    def limit(self):
        return self._limit


class BaseS3ToEvent(ABC):
    def __init__(self, config: S3ToEventConfig, params: S3ToEventParams):
        self.config = config
        self.params = params

    def list_files(self) -> list:
        paginator = self.config.s3.get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': self.params.input_bucket, 'Prefix': self.params.prefix}

        result = []
        for page in paginator.paginate(**operation_parameters):
            result.extend(
                [p for p in page['Contents']
                 if p['Size'] > 0
                 ]
            )
        return result

    def list_output_files(self) -> list:
        paginator = self.config.s3.get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': self.params.output_bucket, 'Prefix': self.params.prefix}

        result = []
        for page in paginator.paginate(**operation_parameters):
            if page.get('Contents') is not None:
                result.extend([BaseS3ToEvent.extract_file_name(f['Key']) for f in page['Contents']])

        return result

    @staticmethod
    def extract_file_name(key: str) -> str:
        return str(key.split('.')[0]).split('/')[-1]

    def transform_to_event(self, ls: list) -> dict:
        return {
            'Records': [
                {'s3': {'bucket': {'name': self.params.input_bucket}, 'object': {'key': list_item['Key'], 'size': list_item['Size']}}}
                for list_item in ls]
        }


class LambdaRunner:
    LAMBDA_FUNCTION_NAME = 'sds-{}-ingestion-ingest-file-ingestion'

    def __init__(self, config: S3ToEventConfig, config_file_name: str) -> None:
        self.config = config
        with open(config_file_name, 'r') as f:
            self.events = json.loads(f.read())

    def run(self):
        for record in self.events['Records']:
            event_record = {'Records': [record]}
            response = self.config.lam.invoke(
                FunctionName=LambdaRunner.LAMBDA_FUNCTION_NAME.format(self.config.env),
                Payload=json.dumps(event_record).encode('utf-8'),
                Qualifier='$LATEST'
            )
            logger.info(f"Lambda run response: {str(response)}")


def s3_to_event_run(config: S3ToEventConfig, params: S3ToEventParams, s3_to_event: BaseS3ToEvent) -> None:
    logger.info(f"App config: {repr(config)}")

    files = s3_to_event.list_files()
    logger.info(f"Collected {len(files)} files")

    output_files = set([f.replace('-' + f.split('-')[-1], '') for f in s3_to_event.list_output_files()])
    logger.info(f"Collected {len(output_files)} output files")

    filtered_files = [f for f in files if BaseS3ToEvent.extract_file_name(f['Key']) not in output_files]
    logger.info(f"Files to process that not exist in output: {len(filtered_files)}")

    event = s3_to_event.transform_to_event(filtered_files[:params.limit])
    logger.info(f"Generated event")

    event_file_name = os.path.join(config.data_path, f's3_event_{config.env}.json')
    with open(event_file_name, 'w') as f:
        f.write(json.dumps(event))
    logger.info(f"Event saved to {event_file_name}")

    # lr = LambdaRunner(app_config, event_file_name)
    # lr.run()
