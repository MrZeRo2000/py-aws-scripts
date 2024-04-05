import argparse
import boto3
from botocore.exceptions import ClientError
import datetime
import re
from dataclasses import dataclass


from logger import get_logger

logger = get_logger(__name__)


class Config:
    def __init__(self, env):
        self.env = env
        self.session = boto3.session.Session(profile_name=env)
        self._s3 = self.session.client('s3')

    @property
    def s3(self):
        return self._s3


@dataclass
class Params:
    bucket: str
    source_key: str
    target_key: str
    date_regexp: str


class SplitByDate:
    def __init__(self, config: Config, params: Params):
        self.config = config
        self.params = params

    def list_files(self) -> list:
        paginator = self.config.s3.get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': self.params.bucket.format(self.config.env), 'Prefix': self.params.source_key}

        result = []
        for page in paginator.paginate(**operation_parameters):
            result.extend([p for p in page['Contents'] if p['Size'] > 0])
        return result

    def file_exists(self, bucket: str, file_name: str) -> bool:
        try:
            _ = self.config.s3.head_object(Bucket=bucket, Key=file_name)
            return True
        except ClientError:
            return False

    def get_mapping(self, files: list) -> list:
        result = []
        for file in files:
            file_path = file['Key']
            file_name = file_path.split('/')[-1]
            match = re.search(self.params.date_regexp, file_name)
            if match is not None:
                date = datetime.datetime.strptime(match.group(0), '%Y-%m-%d')
                target_file_path = f"{self.params.target_key}/year={date.year}/month={date.month:02d}/day={date.day:02d}/{file_name}"
                if not self.file_exists(self.params.bucket.format(self.config.env), target_file_path):
                    result.append((file_path, target_file_path))
                else:
                    logger.info(f"File already exists: {target_file_path}")
            pass

        return result

    def move_file(self, source_key: str, target_key: str):
        copy_source = {
            'Bucket': self.params.bucket.format(self.config.env),
            'Key': source_key
        }
        self.config.s3.copy(copy_source, copy_source['Bucket'], target_key)
        self.config.s3.delete_object(Bucket=copy_source['Bucket'], Key=source_key)

    def execute(self):
        source_files = self.list_files()
        logger.info(f'Found {len(source_files)} source files')

        mapping = self.get_mapping(source_files)
        logger.info(f'Mapping: {str(mapping)}')

        for source_key, target_key in mapping:
            logger.info(f'Moving {source_key} to {target_key}')
            self.move_file(source_key, target_key)
            logger.info(f'Moving completed')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('env')
    parser.add_argument('bucket')
    parser.add_argument('source_key')
    parser.add_argument('target_key')
    parser.add_argument('date_regexp')

    args = parser.parse_args()

    args_params = Params(args.bucket, args.source_key, args.target_key, args.date_regexp)
    logger.info(f"Started with params: {args_params}")

    cfg = Config(args.env)
    sd = SplitByDate(cfg, args_params)

    sd.execute()
