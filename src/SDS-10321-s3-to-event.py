import argparse
from base.base_s3_to_event import BaseS3ToEvent, S3ToEventConfig, S3ToEventParams, s3_to_event_run
from base.logger import get_logger

logger = get_logger(__name__)


class S3ToEvent(BaseS3ToEvent):
    def get_excluded_files(self) -> list:
        return [
            'concardis-sftp-data/6900979(_)4_6900979_20240131_1_R-MYXTRI__ETRAXIC-157680242.240130.000001_timestamp=2024-02-01T05:02:25.csv',
            'concardis-sftp-data/6900979(_)4_6900979_20231228_1_R-MYXTRI__ETRAXIC-157680242.231227.000001_timestamp=2023-12-29T05:02:07.csv',
            'concardis-sftp-data/6900979(_)4_6900979_20231214_1_R-MYXTRI__ETRAXIC-157680242.231213.000001_timestamp=2023-12-15T05:01:43.csv',
            'concardis-sftp-data/6900979(_)4_6900979_20231207_1_R-MYXTRI__ETRAXIC-157680242.231206.000001_timestamp=2023-12-08T05:01:46.csv',
            'concardis-sftp-data/6900979(_)4_6900979_20231123_1_R-MYXTRI__ETRAXIC-157680242.231122.000001_timestamp=2023-11-24T05:01:46.csv',
            'concardis-sftp-data/6900979(_)4_6900979_20231116_1_R-MYXTRI__ETRAXIC-157680242.231115.000001_timestamp=2023-11-17T05:01:28.csv',
            'concardis-sftp-data/6900979(_)4_6900979_20231102_1_R-MYXTRI__ETRAXIC-157680242.231101.000001_timestamp=2023-11-03T05:01:42.csv',
            'concardis-sftp-data/6900979(_)4_6900979_20231026_1_R-MYXTRI__ETRAXIC-157680242.231025.000001_timestamp=2023-10-27T05:01:44.csv',
            'concardis-sftp-data/6900979(_)4_6900979_20231012_1_R-MYXTRI__ETRAXIC-157680242.231011.000001_timestamp=2023-10-13T05:01:41.csv',
            'concardis-sftp-data/6900979(_)4_6900979_20230928_1_R-MYXTRI__ETRAXIC-157680242.230927.000001_timestamp=2023-09-29T05:01:45.csv',
            'concardis-sftp-data/6900979(_)4_6900979_20220603_1_R-MYXTRI__ETRAXIC-153215184.220602.000001_timestamp=2023-10-10T05:05:30.csv',
            'concardis-sftp-data/6900979(_)4_6900979_20220528_1_R-MYXTRI__ETRAXIC-153215184.220527.000001_timestamp=2023-10-10T05:25:16.csv',
            'concardis-sftp-data/6900979(_)4_6900979_20220520_1_R-MYXTRI__ETRAXIC-153215184.220519.000001_timestamp=2023-10-11T05:08:13.csv',
            'concardis-sftp-data/6900979(_)4_6900979_20240202_1_R-MYXTRI__ETRAXIC-157680242.240131.000001_timestamp=2024-02-03T05:02:32.csv',
            'concardis-sftp-data/6900979(_)4_6900979_20240213_1_R-MYXTRI__ETRAXIC-157680242.240212.000001_timestamp=2024-02-14T05:02:26.csv',
            'concardis-sftp-data/6900979(_)4_6900979_20240221_1_R-MYXTRI__ETRAXIC-157680242.240220.000001_timestamp=2024-02-22T05:02:11.csv',
            'concardis-sftp-data/6900979(_)4_6900979_20240229_1_R-MYXTRI__ETRAXIC-157680242.240228.000001_timestamp=2024-03-01T05:02:47.csv',
            'concardis-sftp-data/6900979(_)4_6900979_20240321_1_R-MYXTRI__ETRAXIC-157680242.240320.000001_timestamp=2024-03-22T05:02:27.csv',
            'concardis-sftp-data/6900979(_)4_6900979_20240322_1_R-MYXTRI__ETRAXIC-157680242.240321.000001_timestamp=2024-03-23T05:02:26.csv',
            'concardis-sftp-data/6900979(_)4_6900979_20240326_1_R-MYXTRI__ETRAXIC-157680242.240325.000001_timestamp=2024-03-27T05:02:09.csv',
            'concardis-sftp-data/6900979(_)4_6900979_20240403_1_R-MYXTRI__ETRAXIC-157680242.240402.000001_timestamp=2024-04-04T05:02:24.csv',
            'concardis-sftp-data/6900979(_)4_6900979_20240416_1_R-MYXTRI__ETRAXIC-157680242.240415.000001_timestamp=2024-04-17T05:02:29.csv',
            'concardis-sftp-data/6900979(_)4_6900979_20240416_1_R-MYXTRI__ETRAXIC-157680242.240415.000001_timestamp=2024-04-17T05:02:29.csv',
            'concardis-sftp-data/6900979(_)4_6900979_20240418_1_R-MYXTRI__ETRAXIC-157680242.240417.000001_timestamp=2024-04-19T05:02:35.csv'
        ]


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
