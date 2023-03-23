import dataclasses
import mimetypes
import os
import pathlib
from dataclasses import dataclass
from tempfile import TemporaryDirectory

# noinspection PyPackageRequirements
import ffmpeg
import minio
import minio.commonconfig
import minio.lifecycleconfig
import requests as requests
from urllib3.exceptions import MaxRetryError
from yt_dlp import YoutubeDL

import util
from model import MinioNotConnected, UfysError, UfysRequest, UfysResponse, UfysResponseVideoMetadata


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


def url_format_selector(ctx):
    # sort best to worst
    formats = ctx.get("formats")[::-1]
    # we're looking for h264 mp4 with audio
    for fmt in formats:
        if fmt["acodec"] == "none":
            continue
        if fmt["vcodec"] != "h264":
            continue
        if fmt["ext"] != "mp4":
            continue
        yield dict(
            format_id=fmt["format_id"],
            ext=fmt["ext"],
            requested_formats=[fmt],
            protocol=fmt["protocol"],
            url=fmt["url"]
        )
        # one is enough :)
        return


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
        # try to extract using custom format extractor
        self.ytdl_url = YoutubeDL(dict(
            format=url_format_selector
        ))
        # downloads should just use the default settings
        self.ytdl_dl = YoutubeDL()

    def handle_request(self, req: UfysRequest) -> UfysResponse | UfysError:
        info = self.ytdl_url.extract_info(req.url, download=False)
        type_ = info.get("_type", "video")
        if type_ == "video":
            return self.handle_video(info, req)
        if type_ == "playlist":
            entries = info.get("entries", [])
            if not entries:
                return UfysError("empty-playlist", "no video found in playlist")
            if direct_url := entries[0].get("url"):
                return self.handle_direct_url(direct_url, info)
            if orig_url := entries[0].get("original_url"):
                req.url = orig_url
                return self.handle_request(req)
            return UfysError("unknown-playlist", message="playlist detected, but unable to process")
        return UfysError(code="unknown-type", message="unknown media type")

    @staticmethod
    def meta_from_info(info):
        return dataclasses.asdict(
            UfysResponseVideoMetadata(
                title=info.get("title"),
                creator=info.get("uploader"),
            )
        )

    def handle_video(self, info, req: UfysRequest):
        if direct_url := info.get("url"):
            return self.handle_direct_url(direct_url, info)

        return self.reupload(req.url, req.hash)

    def handle_direct_url(self, url: str, info):
        if not (width := info.get("width")) or not (height := info.get("height")):
            # we don't know the dimensions
            with TemporaryDirectory() as _tmp:
                tmp = pathlib.Path(_tmp)
                file = tmp / "video"
                with requests.get(url, stream=True) as r:
                    r.raise_for_status()
                    with open(file, "wb") as f:
                        for chunk in r.iter_content(chunk_size=8 * 1024):
                            f.write(chunk)
                width, height = self.find_dimensions(file)
        return UfysResponse(
            **self.meta_from_info(info),
            video_url=url,
            width=width,
            height=height
        )

    def reupload(self, url: str, hash_: str):
        # TODO size limit - pass in via request param? (support for external overrides)
        if self.minio is None:
            raise MinioNotConnected()
        with TemporaryDirectory() as tmp:
            with util.chdir(tmp):
                info = self.ytdl_dl.extract_info(url)
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
            if not (width := downloads[0].get("width")) or not (height := downloads[0].get("height")):
                width, height = self.find_dimensions(path)
        # TODO this will probably be wrong when _not_ running in dev
        return UfysResponse(
            **self.meta_from_info(info),
            video_url=result.location,
            width=width,
            height=height,
            reuploaded=True,
        )

    @staticmethod
    def find_dimensions(path: pathlib.Path):
        streams = ffmpeg.probe(path, select_streams="v").get("streams", [])
        assert len(streams) == 1
        return streams[0].get("width"), streams[0].get("height")
