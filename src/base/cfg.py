import os
import boto3
from abc import ABC


class BaseConfig(ABC):
    REGION = 'eu-west-1'

    def __init__(self, env):
        self.env = env
        self.session = boto3.session.Session(profile_name=env, region_name=BaseConfig.REGION)
        self._s3 = self.session.client('s3')
        self._data_path = os.path.join(os.path.dirname(__file__), "../../data/")

    def __repr__(self):
        return f"(env: {self.env}, data_path: {self._data_path} ({os.path.abspath(self.data_path)}))"

    @property
    def data_path(self):
        return self._data_path

    @property
    def s3(self):
        return self._s3


class BaseParams(ABC):
    def __init__(self, env, dry_run=False):
        self.env = env
        self.dry_run = dry_run

    def __repr__(self):
        return f"(env: {self.env})"
