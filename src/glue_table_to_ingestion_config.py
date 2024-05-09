"""
Purpose: convert glue table metadata to ingestion config
Input: file named "table_schema.json"
Output: output file named "glue_table_ingestion_config.json"
"""

import json
import os

TABLE_SCHEMA_FILE_NAME = "table_schema.json"
INGESTION_CONFIG_FILE_NAME = "ingestion_config.json"
FILE_INDENT = " "*5

data_path = os.path.join(os.path.dirname(__file__), "../data/")
table_schema_path = os.path.join(data_path, TABLE_SCHEMA_FILE_NAME)
ingestion_config_path = os.path.join(data_path, INGESTION_CONFIG_FILE_NAME)

with open(table_schema_path, "r") as f:
    json_schema = json.load(f)

ingestion_config = []
for index, item in enumerate(json_schema):
    ingestion_config.append("{")
    ingestion_config.append(f"{FILE_INDENT}\"index\": {index + 1},")
    ingestion_config.append(f"{FILE_INDENT}\"name\": \"{item['Name']}\",")
    ingestion_config.append(f"{FILE_INDENT}\"field_type\": \"{item['Type']}\"")
    ingestion_config.append("}" + ("," if index < len(json_schema) - 1 else ""))

with open(ingestion_config_path, "w") as f:
    f.writelines("\n".join(ingestion_config))
