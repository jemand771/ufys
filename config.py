import os
from dataclasses import dataclass

import util


@dataclass
class ConfigStore:
    MINIO_ACCESS_KEY: str = None
    MINIO_SECRET_KEY: str = None
    MINIO_ENDPOINT: str = None
    MINIO_BUCKET: str = None
    MINIO_SECURE: bool = True
    AAAS_ENDPOINT: str = None
    PROXY_URL: str = None

    @classmethod
    def from_env(cls):
        return util.dataclass_from_dict(cls, os.environ)

    def __post_init__(self):
        # run some checks and emit warnings if stuff goes wrong
        for key in self.__dataclass_fields__:  # type: ignore
            key: str
            if self.__getattribute__(key) is None:
                print(f"warning: ConfigStore.{key} is None")
