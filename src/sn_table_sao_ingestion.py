import datetime

import requests
import logging
import time
import pandas as pd
import zoneinfo
from requests.auth import HTTPBasicAuth
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

logger = logging.getLogger(__name__)

class SNReader:
    ENDPOINT_URL = r"https://sixtprod.service-now.com/api/now/table/service_availability"
    ENDPOINT_DEFAULT_PARAMS = {
        "sysparm_query": r"ORDERBYsys_created_on^startBETWEENjavascript:gs.dateGenerate('2023-11-14','23:59:59')@javascript:gs.endOfToday()^sys_updated_onONToday@javascript:gs.beginningOfToday()@javascript:gs.endOfToday()^ORsys_updated_onONYesterday@javascript:gs.beginningOfYesterday()@javascript:gs.endOfYesterday()^service_offering.parentDYNAMIC4942c2a09390821836e475518bba10d6^type=daily",
        "sysparm_display_value": r"false",
        "sysparm_fields": r"start,end,type,service_offering,service_commitment,absolute_downtime,scheduled_downtime,scheduled_availability,display_name",
    }
    ENDPOINT_ROWS_LIMIT = 50000

    @staticmethod
    def get_paging_params() -> dict:
        offset = 1
        while True:
            yield {
                "sysparm_limit": SNReader.ENDPOINT_ROWS_LIMIT,
                "sysparm_offset": offset
            }
            offset += SNReader.ENDPOINT_ROWS_LIMIT

    def __init__(self, user_name: str, password: str):
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
            for paging_params in SNReader.get_paging_params():
                request_params = {**self.ENDPOINT_DEFAULT_PARAMS, **paging_params}

                logger.info(f"Reading from ServiceNow with params: {request_params}")
                start_time = time.time()
                response = session.get(self.ENDPOINT_URL, params=request_params)
                end_time = time.time()
                logger.info(f"Reading from ServiceNow completed in {(end_time - start_time):.2f} seconds")

                response.raise_for_status()

                response_json = response.json()
                if 'result' not in response_json or len(response_json['result']) == 0:
                    break
                else:
                    yield response_json['result']

class SNTransformer:
    LOCAL_TZ = zoneinfo.ZoneInfo('Europe/Berlin')
    UTC_TZ = zoneinfo.ZoneInfo('UTC')
    SN_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
    TARGET_DATE_FORMAT = '%d:%m:%Y %H:%M:%S'
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
    def transform(rows: list) -> list:
        return [
            {
                "start": SNTransformer.convert_date_str(r["start"]),
                "end": SNTransformer.convert_date_str(r["end"]),
                "type": r["type"].capitalize(),
                "service_offering": r["display_name"].split("|")[0],
                "service_commitment": r["display_name"].split("|")[1] if len(r["display_name"].split("|")) > 1 else "",
                "allowed_downtime": SNTransformer.date_to_duration(r["absolute_downtime"]),
                "scheduled_downtime": SNTransformer.date_to_duration(r["scheduled_downtime"]),
                "scheduled_availability": r["scheduled_availability"],
            }
            for r in rows
        ]


if __name__ == "__main__":
    logger.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)

    logger.addHandler(ch)

    '''
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('user_name')
    parser.add_argument('password')

    args = parser.parse_args()
    logger.info(f'Starting with args: {args}')

    sn_reader = SNReader(user_name=args.user_name, password=args.password)

    for read_rows in sn_reader.read():
        logger.info(f"Read {len(read_rows)} rows")
    logger.info("Done")
    '''
    import os
    import json
    with open(os.path.join(os.path.dirname(__file__), "../data/sn_sao_sample_10.json"), "r") as f:
        read_rows = json.load(f)['result']

    transformed_rows = SNTransformer.transform(read_rows)
    pass

