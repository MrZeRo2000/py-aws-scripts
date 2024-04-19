from botocore.exceptions import ClientError
import datetime
import re

from base.cfg import BaseConfig, BaseParams
from base.logger import get_logger

logger = get_logger(__name__)


class S3SplitByDateConfig(BaseConfig):
    pass


class S3SplitByDateParams(BaseParams):
    def __init__(self, env: str, dry_run: bool, bucket: str, source_key: str, target_key: str, date_regexp: str):
        super().__init__(env, dry_run)
        self.bucket = bucket
        self.source_key = source_key
        self.target_key = target_key
        self.date_regexp = date_regexp


class BaseS3SplitByDate:
    def __init__(self, config: S3SplitByDateConfig, params: S3SplitByDateParams):
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


def s3_split_by_date_run(config: S3SplitByDateConfig, params: S3SplitByDateParams, split_by_date: BaseS3SplitByDate):
    source_files = split_by_date.list_files()
    logger.info(f'Found {len(source_files)} source files')

    mapping = split_by_date.get_mapping(source_files)
    logger.info(f'Mapping: {str(mapping)}')

    for source_key, target_key in mapping:
        logger.info(f'Moving {source_key} to {target_key}')
        if params.dry_run:
            logger.info(f'Move skipped (dry-run)')
        else:
            split_by_date.move_file(source_key, target_key)
            logger.info(f'Moving completed')
