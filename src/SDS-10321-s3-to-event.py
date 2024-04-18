import argparse
from base.base_s3_to_event import BaseS3ToEvent, S3ToEventConfig, S3ToEventParams, s3_to_event_run
from base.logger import get_logger

logger = get_logger(__name__)


class S3ToEvent(BaseS3ToEvent):
    pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('env')
    parser.add_argument('input_bucket')
    parser.add_argument('output_bucket')
    parser.add_argument('prefix')
    parser.add_argument('limit')

    args = parser.parse_args()
    logger.info(f'Starting s3_to_event with args: {args}')

    config = S3ToEventConfig(args.env)
    params = S3ToEventParams(args.env, args.input_bucket, args.output_bucket, args.prefix, int(args.limit))
    logger.info(f'Config: {config}, Params: {params}')

    s3_to_event_run(config, params, S3ToEvent(config, params))
