import logging.config
import sys

import pandas as pd

import sn_table_common_ingestion as common
from awsglue.utils import getResolvedOptions

# Logging
LOGGING_CONFIG = {
    "version": 1,
    "formatters": {
        "human": {
            "class": "logging.Formatter",
            "format": "%(asctime)s:[%(levelname)s:%(lineno)d]: %(message)s"
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "human"
        }
    },
    "root": {
        "level": "INFO",
        "handlers": ["console"]
    },
}

# Load logging config
logging.config.dictConfig(LOGGING_CONFIG)

logger = logging.getLogger(__name__)
common.logger = logger

ENDPOINT_TABLE_NAME = 'service_offering'

ENDPOINT_DEFAULT_PARAMS = {
    "sysparm_query": r"ORDERBY=sys_created_on^parentDYNAMIC4942c2a09390821836e475518bba10d6",
    "sysparm_display_value": r"true",
    "sysparm_exclude_reference_link": r"true",
    "sysparm_fields": r"name,busines_criticality,parent,used_for,operational_status,owned_by,managed_by,support_group,cost_center",
}

ENDPOINT_ROWS_LIMIT = 10000


class SNTransformer:
    CSV_SEPARATOR = ','
    CSV_COLUMNS = [
        'name',
        'parent.service_classification',
        'parent',
        'portfolio_status',
        'service_status',
        'owned_by',
        'delivery_manager',
        'support_group',
        'cost_center',
    ]

    @staticmethod
    def transform_to_list(rows: list) -> list:
        return [
            {
                SNTransformer.CSV_COLUMNS[0]: r['name'],
                SNTransformer.CSV_COLUMNS[1]: r['busines_criticality'],
                SNTransformer.CSV_COLUMNS[2]: r['parent'],
                SNTransformer.CSV_COLUMNS[3]: r['used_for'],
                SNTransformer.CSV_COLUMNS[4]: r['operational_status'],
                SNTransformer.CSV_COLUMNS[5]: r['owned_by'],
                SNTransformer.CSV_COLUMNS[6]: r['managed_by'],
                SNTransformer.CSV_COLUMNS[7]: r['support_group'],
                SNTransformer.CSV_COLUMNS[8]: r['cost_center'],
            } for r in rows
        ]

    @staticmethod
    def transform_to_csv_rows(lst: list) -> list:
        result = [SNTransformer.CSV_SEPARATOR.join(SNTransformer.CSV_COLUMNS)]
        result.extend([SNTransformer.CSV_SEPARATOR.join(list(s.values())) for s in lst])

        return result

    @staticmethod
    def transform_to_pd(lst: list) -> pd.DataFrame:
        return pd.DataFrame(lst).astype(str)

    @staticmethod
    def transform(rows: list) -> pd.DataFrame:
        return SNTransformer.transform_to_pd(
            SNTransformer.transform_to_list(rows)
        )

if __name__ == "__main__":
    args = getResolvedOptions(sys.argv,
                              [
                               'sn_secret_name',
                               's3_output_file_location'
                              ])
    sn_secret_name = args['sn_secret_name']
    s3_output_file_location = args['s3_output_file_location']

    sn_config = common.SNConfig(sn_secret_name, ENDPOINT_TABLE_NAME)
    reader = common.SNReader(
        endpoint_url=sn_config.url, 
        user_name=sn_config.user_name, 
        password=sn_config.password, 
        endpoint_default_params=ENDPOINT_DEFAULT_PARAMS, 
        endpoint_rows_limit=ENDPOINT_ROWS_LIMIT)
    writer = common.SNWriter(s3_output_file_location)
    writer.prepare()
    logger.info(f"Prepared {s3_output_file_location} for writing")

    for read_rows in reader.read():
        # read
        transformed_rows = SNTransformer.transform_to_list(read_rows)
        logger.info(f"Transformed {len(transformed_rows)} rows")

        # transform
        transformed_df = SNTransformer.transform_to_pd(transformed_rows)

        # write
        writer.write_to_bucket(transformed_df)
        logger.info(f"Wrote {len(transformed_df)} rows to bucket")

    logger.info(f"Finished writing to {s3_output_file_location}")