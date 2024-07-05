import os
import sqlglot
import sqlglot.expressions as exp

SELECT_FILE_NAME = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/select.sql")).replace("\\", "/")
ETL_FILE_NAME = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/etl.inc")).replace("\\", "/")


with open(SELECT_FILE_NAME, "r") as f:
    sql = f.read()

column_names = []

for expression in sqlglot.parse_one(sql).find(exp.Select).args["expressions"]:
    if isinstance(expression, exp.Alias):
        column_names.append(expression.text("alias"))
    elif isinstance(expression, exp.Column):
        column_names.append(expression.text("this"))

print(f"Found {len(column_names)} columns")

data_fields = f"CONST DATA_FIELDS=\"{','.join(column_names)}\""
b_data_fields = f"CONST B_DATA_FIELDS=\"{','.join([f'b.{s}' for s in column_names])}\""
array_hash_fields = f"CONST ARRAY_HASH_FIELDS=\"{','.join([f'CAST({s} AS VARCHAR)' for s in column_names])}\""

with open(ETL_FILE_NAME, "w+") as f:
    f.write(data_fields + '\n')
    f.write(b_data_fields + '\n')
    f.write(array_hash_fields + '\n')

print(f"ETL configuration saved to {ETL_FILE_NAME}")
