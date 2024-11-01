"""
Script Name: athena_query
Author: Roman
Date: Nov 01 2024

Description: retrieves athena query by execution id and prints

Parameters:
     env: execution environment (dev, prod)
     query_execution_id: athena query execution id

Output: SQL printed in console
"""


import argparse
from cfg import BaseConfig


class AthenaQuery:
    def __init__(self, config: BaseConfig):
        self.config = config

    def get_query_sql(self, execution_id: str) -> str:
        client = self.config.athena
        response = client.get_query_execution(QueryExecutionId=execution_id)
        return response['QueryExecution']['Query']

    def execute(self, execution_id: str):
        print(self.get_query_sql(execution_id))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('env')
    parser.add_argument('query_execution_id')
    args = parser.parse_args()

    AthenaQuery(BaseConfig(args.env)).execute(args.query_execution_id)
