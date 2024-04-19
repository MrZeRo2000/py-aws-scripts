import argparse
from base.base_s3_split_by_date import s3_split_by_date_run, BaseS3SplitByDate, S3SplitByDateConfig, S3SplitByDateParams
from base.logger import get_logger

logger = get_logger(__name__)


class S3SplitByDate(BaseS3SplitByDate):
    pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('env')
    parser.add_argument('bucket')
    parser.add_argument('source_key')
    parser.add_argument('target_key')
    parser.add_argument('date_regexp')
    parser.add_argument('--dry_run', default=False, required=False, action='store_true')

    args = parser.parse_args()
    logger.info(f'Starting s3_to_event with args: {args}')

    config = S3SplitByDateConfig(args.env)
    params = S3SplitByDateParams(args.env, bool(args.dry_run), args.bucket, args.source_key, args.target_key, args.date_regexp)

    s3_split_by_date_run(config, params, S3SplitByDate(config, params))

    logger.info("Completed")
