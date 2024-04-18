import os
import json
import boto3
import argparse
import datetime
from dateutil.tz import tzutc
from base.logger import get_logger

logger = get_logger(__name__)


class Config:
    INPUT_BUCKET = 'sds-{}-ingest-external-sftp-files'
    OUTPUT_BUCKET = 'sds-{}-ingest-external-sftp-files-out'
    PREFIX = 'adyen-sftp-data/received_payments_report'
    REGION = 'eu-west-1'
    LIMIT = 10

    def __init__(self, env):
        self.env = env
        self.session = boto3.session.Session(profile_name=env)
        self._s3 = self.session.client('s3')
        self._lambda = self.session.client('lambda', region_name=Config.REGION)
        self._data_path = os.path.join(os.path.dirname(__file__), "../data/")

    def __repr__(self):
        return f"(env: {self.env}, data_path: {self._data_path})"

    @property
    def input_bucket(self):
        return self.INPUT_BUCKET.format(self.env)

    @property
    def output_bucket(self):
        return self.OUTPUT_BUCKET.format(self.env)

    @property
    def data_path(self):
        return self._data_path

    @property
    def s3(self):
        return self._s3

    @property
    def lam(self):
        return self._lambda


class S3ToEvent:
    def __init__(self, config: Config):
        self.config = config

    def list_files(self) -> list:
        paginator = self.config.s3.get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': self.config.input_bucket, 'Prefix': Config.PREFIX}

        result = []
        for page in paginator.paginate(**operation_parameters):
            result.extend(
                [p for p in page['Contents']
                 if p['LastModified'] >= datetime.datetime(2023, 11, 1, tzinfo=tzutc())
                 and p['Size'] > 0
                 ]
            )
        return result

    def list_output_files(self) -> list:
        paginator = self.config.s3.get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': self.config.output_bucket, 'Prefix': Config.PREFIX}

        result = []
        for page in paginator.paginate(**operation_parameters):
            if page.get('Contents') is not None:
                result.extend([S3ToEvent.extract_file_name(f['Key']) for f in page['Contents']])

        return result

    @staticmethod
    def extract_file_name(key: str) -> str:
        return str(key.split('.')[0]).split('/')[-1]

    @staticmethod
    def transform_to_event(ls: list) -> dict:
        return {
            'Records': [
                {'s3': {'bucket': {'name': app_config.input_bucket}, 'object': {'key': list_item['Key'], 'size': list_item['Size']}}}
                for list_item in ls]
        }


class LambdaRunner:
    def __init__(self, config: Config, config_file_name: str) -> None:
        self.config = config
        with open(config_file_name, 'r') as f:
            self.events = json.loads(f.read())

    def run(self):
        for record in self.events['Records']:
            event_record = {'Records': [record]}
            response = self.config.lam.invoke(
                FunctionName=f'sds-{self.config.env}-ingestion-ingest-file-ingestion',
                Payload=json.dumps(event_record).encode('utf-8'),
                Qualifier='$LATEST'
            )
            logger.info(f"Lambda run response: {str(response)}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('env')
    args = parser.parse_args()

    app_config = Config(args.env)
    logger.info(f"App config: {repr(app_config)}")

    se = S3ToEvent(app_config)
    files = se.list_files()
    logger.info(f"Collected {len(files)} files")

    output_files = set([f.replace('-' + f.split('-')[-1], '') for f in se.list_output_files()])
    logger.info(f"Collected {len(output_files)} output files")

    filtered_files = [f for f in files if S3ToEvent.extract_file_name(f['Key']) not in output_files]
    logger.info(f"Files to process that not exist in output: {len(filtered_files)}")

    event = S3ToEvent.transform_to_event(filtered_files[:Config.LIMIT])
    logger.info(f"Generated event")

    event_file_name = os.path.join(app_config.data_path, f's3_event_{args.env}.json')
    with open(event_file_name, 'w') as f:
        f.write(json.dumps(event))
    logger.info(f"Event saved to {event_file_name}")

    lr = LambdaRunner(app_config, event_file_name)
    lr.run()
