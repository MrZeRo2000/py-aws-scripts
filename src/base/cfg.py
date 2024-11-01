import os
import boto3
from abc import ABC
from collections import UserDict

class ServiceDict(UserDict):
    def __init__(self, session):
        super().__init__()
        self.session = session

    def __missing__(self, key):
        return self.session.client(key)


class BaseConfig(ABC):
    REGION = 'eu-west-1'

    def __init__(self, env):
        self.env = env
        self.session = boto3.session.Session(profile_name=env, region_name=BaseConfig.REGION)
        self._data_path = os.path.join(os.path.dirname(__file__), "../../data/")

        self._services = ServiceDict(self.session)

    def __repr__(self):
        return f"(env: {self.env}, data_path: {self._data_path} ({os.path.abspath(self.data_path)}))"

    @property
    def data_path(self):
        return self._data_path

    def __getattr__(self, name):
        return self._services.get(name)


class BaseParams(ABC):
    def __init__(self, env, dry_run=False):
        self.env = env
        self.dry_run = dry_run

    def __repr__(self):
        return f"(env: {self.env})"
