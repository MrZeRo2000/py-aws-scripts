import awswrangler as awr
import pyarrow.parquet as pq
import re
from base.cfg import BaseConfig, BaseParams
from base.logger import get_logger

logger = get_logger(__name__)


class Config(BaseConfig):
    pass


class Params(BaseParams):
    def __init__(self, env):
        super(Params, self).__init__(env)
        self.bucket = f"sds-{self.env}-ingestion-store-datalake-public"
        self.source_key = "customersupportmodificationevent"


class SchemaManager:
    def __init__(self, config: Config, params: Params):
        self.config = config
        self.params = params

    def list_files(self) -> list:
        paginator = self.config.s3.get_paginator('list_objects_v2')
        operation_parameters = {'Bucket': self.params.bucket, 'Prefix': self.params.source_key}

        result = []
        for page in paginator.paginate(**operation_parameters):
            result.extend(
                [p for p in page['Contents']
                 if p['Size'] > 0
                 ]
            )
        return result

    @staticmethod
    def order_files(files: list) -> list:
        cf = [(f['Key'], SchemaManager.extract_date(f['Key'])) for f in files]
        return sorted(cf, key=lambda x: x[1])

    @staticmethod
    def extract_date(key: str) -> str:
        f = re.findall(r'year=(\d+)/month=(\d+)/day=(\d+)', key)
        if len(f) == 0:
            return ""
        else:
            return "".join(f[0])

    def get_file_data_types(self, file: str, date_str: str):
        logger.info(f"Reading types for {date_str}: {file}")
        parquet_columns_types, partitions_types = awr.s3.read_parquet_metadata(path=f's3://{self.params.bucket}/{file}', dataset=False)
        # pr = awr.s3.read_parquet(f"s3://{self.params.bucket}/{file}")
        logger.info(f"Reading types completed")
        # return list(zip(list(pr.columns), list(pr.dtypes)))
        return parquet_columns_types

    # [(k, v, files_with_types[2][1].get(k)) for k, v in files_with_types[1][1].items() if k in files_with_types[2][1] and v != files_with_types[2][1][k]]
    def get_differences(self, files: list) -> list:
        result = []
        for i in range(len(files) - 1):
            f1, f2 = files[i], files[i + 1]
            diff = [(k, v, f2[0], f2[1], f2[2].get(k)) for k, v in f1[2].items() if k in f2[2] and v != f2[2][k]]
            if len(diff) > 0:
                logger.info(f"Found differences for {f1} and {f2}: {diff}")
                result.extend(diff)

        return result

    def read_schema(self):
        files = self.list_files()
        ordered_files = SchemaManager.order_files(files)
        files_with_types = [(f[0], f[1], self.get_file_data_types(f[0], f[1])) for f in ordered_files]
        # self.get_file_data_types(ordered_files[0], ordered_files[1])
        differences = self.get_differences(files_with_types)
        logger.info(f"Found differences: {differences}")
        pass


if __name__ == '__main__':
    config = Config('dev')
    params = Params(config.env)
    schema_manager = SchemaManager(config, params)
    schema_manager.read_schema()
    # SchemaManager.extract_date('customersupportmodificationevent/year=2023/month=04/day=19/part-00000-146790af-cce5-4b8d-904d-6130362a0472.c000.snappy.parquet')
