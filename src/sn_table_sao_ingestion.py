import datetime
import requests
import sys
import logging.config
import time
import pandas as pd
import zoneinfo
import uuid
import awswrangler as wr
from requests.auth import HTTPBasicAuth
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
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

class SNConfig:
    def __init__(self, secret_name: str):
        self.secret = wr.secretsmanager.get_secret_json(secret_name)

    @property
    def url(self):
        return self.secret["url"]

    @property
    def user_name(self):
        return self.secret["user_name"]

    @property
    def password(self):
        return self.secret["password"]

class SNReader:
    ENDPOINT_DEFAULT_PARAMS = {
        "sysparm_query": r"ORDERBYsys_created_on^startBETWEENjavascript:gs.dateGenerate('2023-11-14','23:59:59')@javascript:gs.endOfToday()^sys_updated_onONToday@javascript:gs.beginningOfToday()@javascript:gs.endOfToday()^ORsys_updated_onONYesterday@javascript:gs.beginningOfYesterday()@javascript:gs.endOfYesterday()^service_offering.parentDYNAMIC4942c2a09390821836e475518bba10d6^type=daily",
        "sysparm_display_value": r"false",
        "sysparm_fields": r"start,end,type,service_offering,service_commitment,absolute_downtime,scheduled_downtime,scheduled_availability,display_name",
    }
    ENDPOINT_ROWS_LIMIT = 50000

    def __init__(self, endpoint_url, user_name: str, password: str):
        self.endpoint_url = endpoint_url
        self._auth = HTTPBasicAuth(user_name, password)

    def get_session(self) -> requests.Session:
        s = requests.Session()
        s.auth = self._auth

        retries = Retry(
            total=10,
            backoff_factor=0.5,
        )
        s.mount('https://', HTTPAdapter(max_retries=retries))

        return s

    def read(self) -> list:
        with SNReader.get_session(self) as session:
            offset = 1
            while True:
                cycle_params = {
                    "sysparm_limit": SNReader.ENDPOINT_ROWS_LIMIT,
                    "sysparm_offset": offset
                }
                request_params = {**self.ENDPOINT_DEFAULT_PARAMS, **cycle_params}

                logger.info(f"Reading from ServiceNow with params: {request_params}")
                start_time = time.time()
                response = session.get(self.endpoint_url, params=request_params)
                end_time = time.time()
                logger.info(f"Reading from ServiceNow completed in {(end_time - start_time):.2f} seconds")

                response.raise_for_status()

                response_json = response.json()
                if 'result' not in response_json or len(response_json['result']) == 0:
                    logger.info("No data read, exiting")
                    break
                else:
                    response_result = response_json['result']
                    offset += len(response_result)
                    yield response_result

class SNTransformer:
    CSV_SEPARATOR = ','
    CSV_COLUMNS = [
        'start',
        'end',
        'type',
        'service_offering',
        'service_commitment',
        'absolute_downtime',
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
        return pd.DataFrame(lst)

    @staticmethod
    def transform(rows: list) -> pd.DataFrame:
        return SNTransformer.transform_to_pd(
            SNTransformer.transform_to_list(rows)
        )

class SNWriter:
    def __init__(self, bucket: str):
        self._bucket = bucket

    def prepare(self):
        wr.s3.delete_objects(self._bucket)

    def write_to_bucket(self, df: pd.DataFrame):
        file_name = f"{datetime.datetime.now().strftime('%Y%m%d_%H%M%S_%f')}_{uuid.uuid4()}"
        path = f"{self._bucket.rstrip('/')}/{file_name}"
        logger.info(f"Writing to {path}")
        wr.s3.to_parquet(df=df, path=path, index=False)
        logger.info("Written")


if __name__ == "__main__":
    args = getResolvedOptions(sys.argv,
                              ['JOB_NAME',
                               'sn_secret_name',
                               's3_output_file_location'
                              ])
    sn_secret_name = args['sn_secret_name']
    s3_output_file_location = args['s3_output_file_location']
    logger.info(f"Starting with secret {sn_secret_name}, bucket {s3_output_file_location}")

    sn_config = SNConfig(sn_secret_name)
    reader = SNReader(endpoint_url=sn_config.url, user_name=sn_config.user_name, password=sn_config.password)
    writer = SNWriter(s3_output_file_location)
    writer.prepare()
    logger.info(f"Prepared {s3_output_file_location} for writing")

    for read_rows in reader.read():
        transformed_rows = SNTransformer.transform_to_list(read_rows)
        logger.info(f"Transformed {len(transformed_rows)} rows")

        transformed_df = SNTransformer.transform_to_pd(transformed_rows)
        writer.write_to_bucket(transformed_df)
        logger.info(f"Wrote {len(transformed_df)} rows to bucket")

    logger.info(f"Finished writing to {s3_output_file_location}")