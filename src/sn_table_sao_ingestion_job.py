import datetime
import logging.config
import sys
import zoneinfo
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

ENDPOINT_TABLE_NAME = 'service_availability'

ENDPOINT_DEFAULT_PARAMS = {
    "sysparm_query": r"ORDERBYsys_created_on^startBETWEENjavascript:gs.dateGenerate('2023-11-14','23:59:59')@javascript:gs.endOfToday()^sys_updated_onONToday@javascript:gs.beginningOfToday()@javascript:gs.endOfToday()^ORsys_updated_onONYesterday@javascript:gs.beginningOfYesterday()@javascript:gs.endOfYesterday()^service_offering.parentDYNAMIC4942c2a09390821836e475518bba10d6^type=daily",
    "sysparm_display_value": r"false",
    "sysparm_fields": r"start,end,type,service_offering,service_commitment,absolute_downtime,scheduled_downtime,scheduled_availability,display_name",
}

ENDPOINT_ROWS_LIMIT = 10000


class SNTransformer:
    CSV_SEPARATOR = ','
    CSV_COLUMNS = [
        'start',
        'end',
        'type',
        'service_offering',
        'service_commitment',
        'allowed_downtime',
        'scheduled_downtime',
        'scheduled_availability'
    ]
    LOCAL_TZ = zoneinfo.ZoneInfo('Europe/Berlin')
    UTC_TZ = zoneinfo.ZoneInfo('UTC')
    SN_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
    TARGET_DATE_FORMAT = '%d.%m.%Y %H:%M:%S'
    INITIAL_DATE = datetime.datetime(1970,1,1)

    @staticmethod
    def convert_date_str(date_str: str) -> str:
        return datetime.datetime.strptime(date_str, SNTransformer.SN_DATE_FORMAT)\
            .replace(tzinfo=SNTransformer.LOCAL_TZ)\
            .astimezone(SNTransformer.UTC_TZ)\
            .strftime(SNTransformer.TARGET_DATE_FORMAT)

    @staticmethod
    def date_to_duration(date_str: str) -> str:
        return str(int((datetime.datetime.strptime(date_str, SNTransformer.SN_DATE_FORMAT) -
                        SNTransformer.INITIAL_DATE).total_seconds()))

    @staticmethod
    def transform_to_list(rows: list) -> list:
        return [
            {
                SNTransformer.CSV_COLUMNS[0]: SNTransformer.convert_date_str(r["start"]),
                SNTransformer.CSV_COLUMNS[1]: SNTransformer.convert_date_str(r["end"]),
                SNTransformer.CSV_COLUMNS[2]: r["type"].capitalize(),
                SNTransformer.CSV_COLUMNS[3]: r["display_name"].split("|")[0].rstrip(),
                SNTransformer.CSV_COLUMNS[4]: r["display_name"].split("|")[1].lstrip() if len(r["display_name"].split("|")) > 1 else "",
                SNTransformer.CSV_COLUMNS[5]: SNTransformer.date_to_duration(r["absolute_downtime"]),
                SNTransformer.CSV_COLUMNS[6]: SNTransformer.date_to_duration(r["scheduled_downtime"]),
                SNTransformer.CSV_COLUMNS[7]: r["scheduled_availability"],
            }
            for r in rows
        ]

    @staticmethod
    def transform_to_csv_rows(lst: list) -> list:
        result = [SNTransformer.CSV_SEPARATOR.join(SNTransformer.CSV_COLUMNS)]
        result.extend([SNTransformer.CSV_SEPARATOR.join(list(s.values())) for s in lst])

        return result

    @staticmethod
    def transform_to_pd(lst: list) -> pd.DataFrame:
        df = pd.DataFrame(lst).astype(str)
        df = df.astype({
            'allowed_downtime': 'int64',
            'scheduled_downtime': 'int64',
            'scheduled_availability': 'float64',
        })
        return df

    @staticmethod
    def transform(rows: list) -> pd.DataFrame:
        return SNTransformer.transform_to_pd(
            SNTransformer.transform_to_list(rows)
        )

if __name__ == "__main__":
    args = getResolvedOptions(sys.argv,
                              ['JOB_NAME',
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