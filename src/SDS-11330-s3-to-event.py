import argparse
from base.base_s3_to_event import BaseS3ToEvent, S3ToEventConfig, S3ToEventParams, s3_to_event_run_automated
from base.logger import get_logger

logger = get_logger(__name__)


class S3ToEvent(BaseS3ToEvent):
    pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('env')
    parser.add_argument('input_bucket')
    parser.add_argument('input_prefix')
    parser.add_argument('output_bucket')
    parser.add_argument('output_prefix')
    parser.add_argument('error_prefix')
    parser.add_argument('limit')
    parser.add_argument('--dry_run', default=False, required=False, action='store_true')

    args = parser.parse_args()
    logger.info(f'Starting s3_to_event with args: {args}')

    config = S3ToEventConfig(args.env)
    params = S3ToEventParams(
        env=args.env,
        input_bucket=args.input_bucket,
        input_prefix=args.input_prefix,
        output_bucket=args.output_bucket,
        output_prefix=args.output_prefix,
        error_prefix=args.error_prefix,
        limit=int(args.limit),
    )
    logger.info(f'Config: {config}, Params: {params}')

    s3_to_event_run_automated(config, params, S3ToEvent(config, params), args.dry_run)
