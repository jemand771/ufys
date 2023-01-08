import dataclasses
import mimetypes
import os
import pathlib
from dataclasses import dataclass
from tempfile import TemporaryDirectory
from typing import Any

import minio
import minio.commonconfig
import minio.lifecycleconfig
from urllib3.exceptions import MaxRetryError
from yt_dlp import YoutubeDL

import util
from model import MinioNotConnected, UfysRequest, UfysResponse, UfysResponseVideoMetadata


@dataclass
class ConfigStore:
    MINIO_ACCESS_KEY: str = None
    MINIO_SECRET_KEY: str = None
    MINIO_ENDPOINT: str = None
    MINIO_BUCKET: str = None
    MINIO_SECURE: bool = True

    @classmethod
    def from_env(cls):
        return util.dataclass_from_dict(cls, os.environ)

    def __post_init__(self):
        # run some checks and emit warnings if stuff goes wrong
        for key in self.__dataclass_fields__:  # type: ignore
            key: str
            if self.__getattribute__(key) is None:
                print(f"warning: ConfigStore.{key} is None")


class Worker:
    config: ConfigStore
    minio: "minio.Minio | None" = None

    def __init__(self, config: ConfigStore = None):
        self.config = config or ConfigStore()
        try:
            self.minio = minio.Minio(
                endpoint=self.config.MINIO_ENDPOINT,
                access_key=self.config.MINIO_ACCESS_KEY,
                secret_key=self.config.MINIO_SECRET_KEY,
                secure=self.config.MINIO_SECURE
            )
            if not self.minio.bucket_exists(self.config.MINIO_BUCKET):
                print(f"warning: bucket {config.MINIO_BUCKET} doesn't exist")
                self.minio = None
        except TypeError:
            print("warning: minio not connected (configuration error)")
        except AttributeError:
            print("warning: minio not connected (bucket error)")
        except MaxRetryError:
            print("warning: minio not connected (timeout)")
        # TODO set access policy
        # setting up anonymous access looks painful (json string), and only partially auto-configuring the bucket
        # might yield unexpected results. I'll either re-add this or remove it entirely
        # else:
        #     self.minio.set_bucket_lifecycle(
        #         self.config.MINIO_BUCKET,
        #         minio.lifecycleconfig.LifecycleConfig(
        #             [
        #                 minio.lifecycleconfig.Rule(
        #                     status=minio.commonconfig.ENABLED,
        #                     rule_filter=minio.commonconfig.Filter(prefix=""),
        #                     expiration=minio.lifecycleconfig.Expiration(days=1)
        #                 )
        #             ]
        #         )
        #     )
        self.ytdl = YoutubeDL()

    def handle_request(self, req: UfysRequest) -> UfysResponse:
        info = self.ytdl.extract_info(req.url, download=False)
        meta = dataclasses.asdict(
            UfysResponseVideoMetadata(
                title=info.get("title"),
                creator=info.get("uploader"),
            )
        )
        if fmt := self.get_best_format(info["formats"]):
            return UfysResponse(
                **meta,
                width=fmt["width"],
                height=fmt["height"],
                video_url=fmt.get("url"),
            )
        url, download = self.reupload(req.url, req.hash)
        return UfysResponse(
            **meta,
            video_url=url,
            width=download["width"],
            height=download["height"],
            reuploaded=True,
        )

    def reupload(self, url: str, hash_: str):
        # TODO size limit - pass in via request param? (support for external overrides)
        if self.minio is None:
            raise MinioNotConnected()
        with TemporaryDirectory() as tmp:
            with util.chdir(tmp):
                info = self.ytdl.extract_info(url)
            downloads = info.get("requested_downloads", [])
            assert len(downloads) == 1
            path = pathlib.Path(downloads[0]["filepath"])
            mime, _ = mimetypes.guess_type(path)
            result = self.minio.fput_object(
                bucket_name=self.config.MINIO_BUCKET,
                object_name=hash_ + path.suffix,
                file_path=str(path),
                content_type=mime
            )
        # TODO this will probably be wrong when _not_ running in dev
        return result.location, downloads[0]

    def get_best_format(self, formats: list[dict[str, Any]]):
        embeddable_formats = [
            # welcome back to "pycharm's linter and autoformatter are having an argument".
            # pycharm, bringing you nonsensical warnings AND ugly code since 2010
            fmt for fmt in formats if
            self.is_valid_tbr(fmt.get("tbr"))
            and self.codec_okay(fmt.get("acodec"))
            and self.codec_okay(fmt.get("vcodec"))
        ]
        if not embeddable_formats:
            return None
        return max(embeddable_formats, key=lambda fmt: float(fmt["tbr"]))

    @staticmethod
    def codec_okay(codec: str) -> bool:
        if not codec:
            return False
        if codec.lower() == "none":
            return False
        return True

    @staticmethod
    def is_valid_tbr(string: str) -> bool:
        if not string:
            return False
        try:
            if float(string) < 1:
                return False
        except ValueError:
            return False
        return True
