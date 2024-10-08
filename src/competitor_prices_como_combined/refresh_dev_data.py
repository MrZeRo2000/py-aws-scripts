"""
Purpose: refresh data for yield competitor prices from PROD to DEV
Parameters: None
"""
import awswrangler as wr
import boto3
import datetime
import os
from base.logger import get_logger

logger = get_logger(__name__)

MAX_FILES = 2
MAX_DAYS = 2

ENV_PROD = 'prod'
ENV_DEV = 'dev'
envs = [ENV_PROD, ENV_DEV]

DATA_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/")) + os.path.sep

def refresh_date(date: datetime.datetime):
    date_path = date.strftime('year=%Y/month=%m/day=%d/')
    folder_paths = {env:
                        f's3://sds-{env}-ingest-external-sftp-files-out/yield-sftp-user/CompetitorPrice/{date_path}'
                    for env in envs}
    logger.info("Folder paths:")
    logger.info(folder_paths)

    prod_files = wr.s3.list_objects(path=folder_paths[ENV_PROD], boto3_session=sessions[ENV_PROD],
                                    s3_additional_kwargs={'MaxKeys': MAX_FILES})[:MAX_FILES]
    file_names = [f.split('/')[-1] for f in prod_files]
    local_files = []

    # download from prod
    for f in zip(prod_files, file_names):
        local_file = f"{DATA_PATH}{f[1]}"
        logger.info(f"Downloading {f[0]} to {local_file}")
        wr.s3.download(path=f[0], local_file=local_file, boto3_session=sessions[ENV_PROD])
        local_files.append(local_file)

    # clean up destination
    logger.info(f"Cleaning up {folder_paths[ENV_DEV]}")
    wr.s3.delete_objects(path=folder_paths[ENV_DEV], boto3_session=sessions[ENV_DEV])

    # copy to destination
    for f in zip(local_files, file_names):
        s3_file = f"{folder_paths[ENV_DEV]}{f[1]}"
        logger.info(f"uploading {f[0]} to {s3_file}")
        wr.s3.upload(local_file=f[0], path=s3_file, boto3_session=sessions[ENV_DEV])


sessions = {env:boto3.session.Session(profile_name=env) for env in envs}

current_date = datetime.datetime.now()
refresh_dates = [current_date - datetime.timedelta(days=offset) for offset in list(range(MAX_DAYS))]

# main cycle
for d in refresh_dates:
    logger.info(f"=== Refreshing {str(d)} ===")
    refresh_date(d)
    logger.info(f"=== Completed {str(d)} ===")
