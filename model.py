import hashlib
import json
from dataclasses import asdict, dataclass


@dataclass
class UfysRequest:
    url: str

    @property
    def hash(self):
        return hashlib.sha1(
            json.dumps(
                asdict(self),
                ensure_ascii=False
            ).encode("utf-8")
        ).hexdigest()


@dataclass
class UfysResponseMetadata:
    title: str | None
    creator: str | None
    site: str | None


@dataclass
class UfysResponse(UfysResponseMetadata):
    video_url: str
    width: int
    height: int
    reuploaded: bool = False


@dataclass
class UfysError(Exception):
    code: str
    message: str = ""


class MinioNotConnected(Exception):
    pass
