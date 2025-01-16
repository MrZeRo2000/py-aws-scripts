import datetime
import sys
import zoneinfo
import json
import pandas as pd

import sn_table_common_ingestion as common
from awsglue.utils import getResolvedOptions

logger = common.get_logger(__name__)
common.logger = logger

ENDPOINT_TABLE_NAME = 'cmdb_ci_outage'

'''
# delta
ENDPOINT_DEFAULT_PARAMS = {
    "sysparm_query": r"cmdb_ci.ref_service_offering.parentDYNAMIC4942c2a09390821836e475518bba10d6^ORcmdb_ciDYNAMIC899e2966c38e56103eb36420a0013154^ORtype=planned^ORu_affected_servicesISNOTEMPTY^sys_updated_onONToday@javascript:gs.beginningOfToday()@javascript:gs.endOfToday()^ORsys_updated_onONYesterday@javascript:gs.beginningOfYesterday()@javascript:gs.endOfYesterday()",
    "sysparm_display_value": r"true",
    "sysparm_exclude_reference_link": r"true",
    "sysparm_fields": r"number,cmdb_ci,begin,end,duration,task_number,short_description,type,u_custom_export_fields,u_affected_services",
}
'''

# full
ENDPOINT_DEFAULT_PARAMS = {
    "sysparm_query": r"ORDERBYsys_created_on^cmdb_ci.ref_service_offering.parentDYNAMIC4942c2a09390821836e475518bba10d6%5EORcmdb_ciDYNAMIC899e2966c38e56103eb36420a0013154%5EORtype%3Dplanned%5EORu_affected_servicesISNOTEMPTY",
    "sysparm_display_value": r"true",
    "sysparm_exclude_reference_link": r"true",
    "sysparm_fields": r"number,cmdb_ci,begin,end,duration,task_number,short_description,type,u_custom_export_fields,u_affected_services",
}

ENDPOINT_ROWS_LIMIT = 10000


class SNTransformer:
    CSV_SEPARATOR = ','
    CSV_COLUMNS = [
        'number',
        'cmdb_ci',
        'type',
        'begin',
        'end',
        'task_number',
        'task_number.cmdb_ci',
        'task_number.short_description',
        'task_number.ref_incident.u_workstation',
        'duration',
        'u_affected_services',
    ]
    LOCAL_TZ = zoneinfo.ZoneInfo('Europe/Berlin')
    UTC_TZ = zoneinfo.ZoneInfo('UTC')
    SN_DATE_FORMAT = '%d.%m.%Y %H:%M:%S'
    TARGET_DATE_FORMAT = '%d.%m.%Y %H:%M:%S'

    @staticmethod
    def convert_date_str(date_str: str) -> str:
        return datetime.datetime.strptime(date_str, SNTransformer.SN_DATE_FORMAT)\
            .replace(tzinfo=SNTransformer.LOCAL_TZ)\
            .astimezone(SNTransformer.UTC_TZ)\
            .strftime(SNTransformer.TARGET_DATE_FORMAT)

    @staticmethod
    def transform_to_list(rows: list) -> list:
        return [
            {
                SNTransformer.CSV_COLUMNS[0]: r["number"],
                SNTransformer.CSV_COLUMNS[1]: r["cmdb_ci"],
                SNTransformer.CSV_COLUMNS[2]: r["type"],
                SNTransformer.CSV_COLUMNS[3]: SNTransformer.convert_date_str(r["begin"]) if r["begin"] != "" else "",
                SNTransformer.CSV_COLUMNS[4]: SNTransformer.convert_date_str(r["end"]) if r["end"] != "" else "",
                SNTransformer.CSV_COLUMNS[5]: r["task_number"],
                SNTransformer.CSV_COLUMNS[6]: u["task_number.cmdb_ci"],
                SNTransformer.CSV_COLUMNS[7]: u["task_number.short_description"],
                SNTransformer.CSV_COLUMNS[8]: u["task_number.ref_incident.u_workstation"],
                SNTransformer.CSV_COLUMNS[9]: u["duration_seconds"],
                SNTransformer.CSV_COLUMNS[10]: r["u_affected_services"],
            }
            for r in rows
            if (u := json.loads(r['u_custom_export_fields']))
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
            'duration': 'int64'
        })
        return df

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