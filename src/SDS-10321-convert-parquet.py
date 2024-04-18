import datetime
from dateutil.tz import tzutc
import awswrangler as wr
import boto3
import argparse
import concurrent.futures
from base.logger import get_logger

logger = get_logger(__name__)


class Config:
    REGION = 'eu-west-1'
    LIMIT = 10

    def __init__(self, env, bucket, prefix):
        self.env = env
        self.bucket = bucket
        self.prefix = prefix
        self.session = boto3.session.Session(profile_name=env)
        self._s3 = self.session.client('s3')

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self.env}: {self.bucket}: {self.prefix}>'

    @property
    def s3(self):
        return self._s3


class ConvertParquet:
    def __init__(self, config: Config):
        self.config = config

    def list_file_names(self) -> list:
        paginator = self.config.s3.get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': self.config.bucket, 'Prefix': self.config.prefix}

        result = []
        current_date = datetime.datetime.now()
        last_modified_date = datetime.datetime(current_date.year, current_date.month, current_date.day, tzinfo=tzutc())
        for page in paginator.paginate(**operation_parameters):
            result.extend(
                [p['Key']
                 for p in page['Contents']
                 # if p['Size'] > 0 and p['LastModified'] < last_modified_date
                 ]
            )
        return result

    def process_file(self, file_name):
        logger.info(f'Processing file {file_name}')
        try:
            parquet_file_name = f"s3://{self.config.bucket}/{file_name}"

            df = wr.s3.read_parquet(parquet_file_name, boto3_session=self.config.session)
            df['collector_number'] = df['collector_number'].astype('string')
            df['POS_entry_mode'] = df['POS_entry_mode'].astype('string')
            # https://github.com/aws/aws-sdk-pandas/pull/1057
            # pyarrow_additional_kwargs={'flavor': None} to keep spaces in column name
            wr.s3.to_parquet(df, parquet_file_name, pyarrow_additional_kwargs={'flavor': None}, boto3_session=self.config.session)

            logger.info(f'Processed file {file_name}')
        except Exception as e:
            logger.error(f"Error processing file {file_name}: {e}")
            raise e

    def execute(self):
        file_names = self.list_file_names()
        for file_name in file_names:
            self.process_file(file_name)

    def execute_parallel(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            fs = [executor.submit(self.process_file, file) for file in self.list_file_names()]
            concurrent.futures.wait(fs)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('env')
    parser.add_argument('bucket')
    parser.add_argument('prefix')
    args = parser.parse_args()

    app_config = Config(args.env, args.bucket, args.prefix)
    logger.info(f"App config: {app_config}")

    ConvertParquet(app_config).execute_parallel()
