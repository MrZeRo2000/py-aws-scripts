"""
Script Name: etl_utils
Author: Roman
Date: Sep 06 2024

Description: Generates .inc file contents for Athena ETL tool

Parameters:
     --use_athena (optional): boolean, default False, runs Athena Query to retrieve field named from SQL,
     otherwise an SQL parser is used

Input: "select.sql" file SQL statement
Output: "etl.inc" with fields
"""


import os
from typing import List
import argparse
import awswrangler as wr
import boto3

import sqlglot
import sqlglot.expressions as exp

SELECT_FILE_NAME = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/select.sql")).replace("\\", "/")
ETL_FILE_NAME = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/etl.inc")).replace("\\", "/")


def read_sql() -> str:
    with open(SELECT_FILE_NAME, "r") as f:
        sql = f.read()
    return sql

def parse_column_names_sql(sql: str) -> List[str]:
    column_names = []

    for expression in sqlglot.parse_one(sql).find(exp.Select).args["expressions"]:
        if isinstance(expression, exp.Alias):
            column_names.append(expression.text("alias"))
        elif isinstance(expression, exp.Column):
            column_names.append(expression.text("this"))

    print(f"Found {len(column_names)} columns")
    return column_names

def parse_column_names_athena(sql: str) -> List[str]:
    session = boto3.session.Session(profile_name='prod', region_name='eu-west-1')
    df = wr.athena.read_sql_query(
        sql,
        ctas_approach=False,
        unload_approach=False,
        database='bi_shop',
        boto3_session=session)

    print(f"Retrieved {len(df.columns)} columns from athena query")
    return list(df.columns)

def write_etl_configuration(column_names):
    data_fields = f"CONST DATA_FIELDS=\"{','.join(column_names)}\""
    b_data_fields = f"CONST B_DATA_FIELDS=\"{','.join([f'b.{s}' for s in column_names])}\""
    array_hash_fields = f"CONST ARRAY_HASH_FIELDS=\"{','.join([f'CAST({s} AS VARCHAR)' for s in column_names])}\""

    with open(ETL_FILE_NAME, "w+") as f:
        f.write(data_fields + '\n')
        f.write(b_data_fields + '\n')
        f.write(array_hash_fields + '\n')

    print(f"ETL configuration saved to {ETL_FILE_NAME}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--use_athena', default=False, required=False, action='store_true')

    args = parser.parse_args()
    if args.use_athena:
        parser = parse_column_names_athena
    else:
        parser = parse_column_names_sql

    etl_sql = read_sql()
    etl_column_names = parser(etl_sql)
    write_etl_configuration(etl_column_names)
