import os
import json
import time
from abc import ABC
from base.cfg import BaseConfig, BaseParams
from base.logger import get_logger

logger = get_logger(__name__)


class S3ToEventConfig(BaseConfig):
    def __init__(self, env):
        super().__init__(env)
        self._lambda = self.session.client('lambda', region_name=S3ToEventConfig.REGION)

    @property
    def lam(self):
        return self._lambda


class S3ToEventParams(BaseParams):
    def __init__(self, env: str, input_bucket: str, input_prefix: str, output_bucket: [str, None], output_prefix: str, error_prefix: [str, None], limit: int):
        super().__init__(env)
        self._input_bucket = input_bucket
        self._input_prefix = input_prefix
        self._output_bucket = output_bucket
        self._output_prefix = output_prefix
        self._error_prefix = error_prefix
        self._limit = limit

    @property
    def input_bucket(self):
        return self._input_bucket.format(self.env)

    @property
    def input_prefix(self):
        return self._input_prefix

    @property
    def output_bucket(self):
        return self._output_bucket.format(self.env)

    @property
    def output_prefix(self):
        return self._output_prefix

    @property
    def error_prefix(self):
        return self._error_prefix

    @property
    def limit(self):
        return self._limit


class BaseS3ToEvent(ABC):
    def __init__(self, config: S3ToEventConfig, params: S3ToEventParams):
        self.config = config
        self.params = params

    def get_excluded_files(self) -> list:
        return []

    def list_files(self) -> list:
        paginator = self.config.s3.get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': self.params.input_bucket, 'Prefix': self.params.input_prefix}

        result = []
        for page in paginator.paginate(**operation_parameters):
            result.extend(
                [p for p in page['Contents']
                 if p['Size'] > 0
                 ]
            )
        return result

    @staticmethod
    def list_bucket_file_names(s3, bucket_name: str, file_prefix: str) -> list:
        result = []
        paginator = s3.get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': bucket_name, 'Prefix': file_prefix}

        for page in paginator.paginate(**operation_parameters):
            if page.get('Contents') is not None:
                result.extend([f['Key'] for f in page['Contents']])

        return result

    def list_error_files(self) -> list:
        result = []
        if self.params.error_prefix is None:
            return result
        else:
            paginator = self.config.s3.get_paginator('list_objects_v2')
            operation_parameters = {'Bucket': self.params.input_bucket, 'Prefix': self.params.error_prefix}

            for page in paginator.paginate(**operation_parameters):
                if page.get('Contents') is not None:
                    result.extend([f['Key'] for f in page['Contents']])

            return result

    @staticmethod
    def extract_file_name(key: str) -> str:
        # return str(key.split('.')[0]).split('/')[-1]
        return key.split('/')[-1].split('_timestamp')[0]

    def get_files_to_process(self) -> list:
        files = self.list_files()
        logger.info(f"Collected {len(files)} input files")

        raw_output_files = BaseS3ToEvent.list_bucket_file_names(self.config.s3, self.params.output_bucket, self.params.output_prefix)
        output_files = set([BaseS3ToEvent.extract_file_name(f) for f in raw_output_files])
        logger.info(f"Collected {len(output_files)} output files")

        raw_error_files = BaseS3ToEvent.list_bucket_file_names(self.config.s3, self.params.input_bucket, self.params.error_prefix)
        error_files = set([BaseS3ToEvent.extract_file_name(f) for f in raw_error_files])
        logger.info(f"Collected {len(error_files)} error files")

        # get all files which are not in the output files, if error files are specified, use then also as filter
        process_files = [f for f in files if BaseS3ToEvent.extract_file_name(f['Key']) not in output_files]
        if len(error_files) > 0:
            process_files = [f for f in process_files if BaseS3ToEvent.extract_file_name(f['Key']) in error_files]
        logger.info(f"Files to process: {len(process_files)}")

        filtered_files = [p for p in process_files if p['Key'] not in self.get_excluded_files()]
        logger.info(f"Filtered files to process: {len(filtered_files)}")

        return filtered_files

    def transform_to_event(self, ls: list) -> dict:
        return {
            'Records': [{
                'body': json.dumps({
                    'Records': [
                        {'s3': {'bucket': {'name': self.params.input_bucket}, 'object': {'key': list_item['Key'], 'size': list_item['Size']}}}
                        ]
                })
            } for list_item in ls]
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


def s3_to_event_run(config: S3ToEventConfig, params: S3ToEventParams, s3_to_event: BaseS3ToEvent,
                    dry_run=False) -> None:
    logger.info(f"App config: {repr(config)}")

    files_to_process = s3_to_event.get_files_to_process()

    if len(files_to_process) == 0:
        logger.info(f"No files to process")
    else:
        event = s3_to_event.transform_to_event(files_to_process[:params.limit])
        logger.info(f"Generated event")

        event_file_name = os.path.join(config.data_path, f's3_event_{config.env}.json')
        with open(event_file_name, 'w') as f:
            f.write(json.dumps(event))
        logger.info(f"Event saved to {event_file_name}")

        if dry_run:
            logger.info(f"Dry run, skipping actions")
        else:
            lr = LambdaRunner(config, event_file_name)
            lr.run()


def s3_to_event_run_automated(config: S3ToEventConfig, params: S3ToEventParams, s3_to_event: BaseS3ToEvent,
                              dry_run=False) -> None:
    logger.info(f"App config: {repr(config)}")

    files_to_process = s3_to_event.get_files_to_process()

    while len(files_to_process) > 0:
        event = s3_to_event.transform_to_event(files_to_process[:params.limit])
        logger.info(f"Generated event")

        event_file_name = os.path.join(config.data_path, f's3_event_{config.env}.json')
        with open(event_file_name, 'w') as f:
            f.write(json.dumps(event))
        logger.info(f"Event saved to {event_file_name}")

        if dry_run:
            logger.info(f"Dry run, skipping actions")
            return
        else:
            num_last_files_to_process = len(files_to_process)

            lr = LambdaRunner(config, event_file_name)
            lr.run()

            while len(files_to_process) > 0:
                logger.info(f"Files to process: {len(files_to_process)}, sleeping ...")
                time.sleep(10)

                files_to_process = s3_to_event.get_files_to_process()
                if len(files_to_process) == num_last_files_to_process - params.limit:
                    logger.info(f"Files to process: {len(files_to_process)}, waking up")
                    break
