import dataclasses
import mimetypes
import os
import pathlib
import urllib.parse
from dataclasses import dataclass
from tempfile import TemporaryDirectory

# noinspection PyPackageRequirements
import ffmpeg
import minio
import minio.commonconfig
import minio.lifecycleconfig
import requests as requests
import yt_dlp.utils
from bs4 import BeautifulSoup
from urllib3.exceptions import MaxRetryError
from yt_dlp import YoutubeDL

import telemetry
import util
from model import MinioNotConnected, UfysError, UfysRequest, UfysResponse, UfysResponseVideoMetadata

YTDL_OPTS = dict(
    progress_with_newline=True,
)


@dataclass
class ConfigStore:
    MINIO_ACCESS_KEY: str = None
    MINIO_SECRET_KEY: str = None
    MINIO_ENDPOINT: str = None
    MINIO_BUCKET: str = None
    MINIO_SECURE: bool = True
    AAAS_ENDPOINT: str = None

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
        try:
            if fmt["acodec"] == "none":
                continue
            if fmt["vcodec"] != "h264":
                continue
            if fmt["ext"] != "mp4":
                continue
            # TODO fall back if this is _too_ bad
            yield dict(
                format_id=fmt["format_id"],
                ext=fmt["ext"],
                requested_formats=[fmt],
                protocol=fmt["protocol"],
                url=fmt["url"]
            )
            # one is enough :)
            return
        except KeyError:
            # incomplete info -> we probably didn't want this anyway
            continue


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
        self.ytdl_url = YoutubeDL(
            dict(
                **YTDL_OPTS,
                # TODO these should be configurable... per request? (e.g. exclude-h265 param)
                format=url_format_selector,
            )
        )
        self.ytdl_url.extract_info = telemetry.trace_function(self.ytdl_url.extract_info)
        # downloads should just use the default settings
        self.ytdl_raw = YoutubeDL(YTDL_OPTS)
        self.ytdl_raw.extract_info = telemetry.trace_function(self.ytdl_raw.extract_info)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "ufys/0.0.0 (https://github.com/jemand771/ufys)"})

    @telemetry.trace_function
    def handle_request(self, req: UfysRequest) -> UfysResponse | UfysError:
        # TODO this should probably be a fancy modular oop thing instead of a giant switch case
        parts = urllib.parse.urlparse(req.url)
        if parts.hostname == "asciinema.org":
            id_, = parts.path.removeprefix("/a/").split("/")
            return self.handle_request_asciinema(id_, req.hash)
        return self.handle_request_ytdl(req)

    def handle_request_ytdl(self, req: UfysRequest) -> UfysResponse | UfysError:
        info = self.ytdl_raw.extract_info(req.url, download=False)
        type_ = info.get("_type", "video")
        if type_ == "video":
            return self.handle_video(req)
        if type_ == "playlist":
            entries = info.get("entries", [])
            if not entries:
                return UfysError("empty-playlist", "no video found in playlist")
            if direct_url := entries[0].get("url"):
                return self.handle_direct_url(direct_url, info)
            if orig_url := entries[0].get("original_url"):
                req.url = orig_url
                return self.handle_request_ytdl(req)
            return UfysError("unknown-playlist", message="playlist detected, but unable to process")
        return UfysError(code="unknown-type", message="unknown media type")

    @staticmethod
    def meta_from_info(info):
        return dataclasses.asdict(
            UfysResponseVideoMetadata(
                title=info.get("title"),
                creator=info.get("uploader"),
                site=extractor if (extractor := info.get("extractor_key")) != "Generic" else None,
            )
        )

    @telemetry.trace_function
    def handle_video(self, req: UfysRequest):
        try:
            info = self.ytdl_url.extract_info(req.url, download=False)
            if not (direct_url := info.get("url")):
                raise yt_dlp.utils.DownloadError("extractor returned no url")
        except yt_dlp.utils.DownloadError:
            return self.reupload_ytdl(req.url, req.hash)
        return self.handle_direct_url(direct_url, info)

    @telemetry.trace_function
    def handle_direct_url(self, url: str, info):
        if not (width := info.get("width")) or not (height := info.get("height")):
            # we don't know the dimensions
            width, height = self.find_dimensions_from_url(url)
        return UfysResponse(
            **self.meta_from_info(info),
            video_url=url,
            width=width,
            height=height
        )

    def reupload_ytdl(self, url: str, hash_: str):
        # TODO size limit - pass in via request param? (support for external overrides)
        if self.minio is None:
            raise MinioNotConnected()
        with TemporaryDirectory() as tmp:
            with util.chdir(tmp):
                info = self.ytdl_raw.extract_info(url)
            downloads = info.get("requested_downloads", [])
            assert len(downloads) == 1
            path = pathlib.Path(downloads[0]["filepath"])
            width = downloads[0].get("width")
            height = downloads[0].get("height")
            location = self.reupload(path, hash_)
            return self.make_reupload_response(
                path=path,
                location=location,
                meta=self.meta_from_info(info),
                dim=(width, height)
            )

    @telemetry.trace_function
    def reupload(self, path: pathlib.Path, hash_: str):
        mime, _ = mimetypes.guess_type(path)
        result = self.minio.fput_object(
            bucket_name=self.config.MINIO_BUCKET,
            object_name=hash_ + path.suffix,
            file_path=str(path),
            content_type=mime
        )
        return result.location or self.get_location(result.object_name)

    def make_reupload_response(
        self, path: pathlib.Path, location: str, meta: dict | None, dim: tuple | None
    ) -> UfysResponse:
        width = height = None
        if dim is not None:
            width, height = dim
        if width is None or height is None:
            width, height = self.find_dimensions_from_file(path)
        return UfysResponse(
            **meta or {},
            video_url=location,
            width=width,
            height=height,
            reuploaded=True,
        )

    def get_location(self, object_name):
        protocol = "https" if self.config.MINIO_SECURE else "http"
        return f"{protocol}://{self.config.MINIO_ENDPOINT}/{self.config.MINIO_BUCKET}/{object_name}"

    @telemetry.trace_function
    def find_dimensions_from_url(self, url: str):
        with TemporaryDirectory() as _tmp:
            tmp = pathlib.Path(_tmp)
            file = tmp / "video"
            with self.session.get(url, stream=True) as r:
                r.raise_for_status()
                with open(file, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8 * 1024):
                        f.write(chunk)
            return self.find_dimensions_from_file(file)

    @staticmethod
    @telemetry.trace_function
    def find_dimensions_from_file(path: pathlib.Path):
        streams = ffmpeg.probe(path, select_streams="v").get("streams", [])
        assert len(streams) == 1
        return streams[0].get("width"), streams[0].get("height")

    @telemetry.trace_function
    def to_mp4(self, source: pathlib.Path, dest: pathlib.Path):
        ffmpeg.input(str(source)).output(str(dest)).run()

    @telemetry.trace_function
    def handle_request_asciinema(self, id_, hash_) -> UfysResponse | UfysError:
        if self.config.AAAS_ENDPOINT is None:
            return UfysError(code="config-error", message="AAAS_ENDPOINT not set")
        r = requests.get(f"https://asciinema.org/a/{id_}.cast?dl=1")
        r.raise_for_status()
        r = requests.post(self.config.AAAS_ENDPOINT, data=r.content)
        r.raise_for_status()
        with TemporaryDirectory() as _tmp:
            gif = pathlib.Path(_tmp) / "render.gif"
            mp4 = pathlib.Path(_tmp) / "render.mp4"
            with open(gif, "wb") as file:
                file.write(r.content)
            self.to_mp4(source=gif, dest=mp4)
            location = self.reupload(mp4, hash_)
            return self.make_reupload_response(
                path=mp4,
                location=location,
                dim=None,
                meta=dict(
                    **self.asciinema_get_metadata(id_=id_),
                    site="asciinema"
                )
            )

    @telemetry.trace_function
    def asciinema_get_metadata(self, id_):
        # TODO marie is looking into an api for this
        r = requests.get(f"https://asciinema.org/a/{id_}")
        r.raise_for_status()
        soup = BeautifulSoup(r.content, "html.parser")
        title = soup.find("meta", dict(property="og:title")).attrs.get("content")
        user_href = soup.find("span", {"class": "author-avatar"}).find("a").attrs.get("href")
        r = requests.get(f"https://asciinema.org{user_href}")
        r.raise_for_status()
        soup = BeautifulSoup(r.content, "html.parser")
        user_string = soup.find("h1").find(string=True, recursive=False).get_text().strip()
        return dict(
            title=title,
            creator=user_string,
        )
