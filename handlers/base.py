import dataclasses
import re
import urllib.parse
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING

# noinspection PyPackageRequirements
import ffmpeg
import requests

import telemetry
from model import UfysRequest, UfysResponse, UfysResponseMetadata

if TYPE_CHECKING:
    from worker import Worker


class RequestHandler:
    regex: re.Pattern | None = None
    hostnames: list[str] | None = None

    def __init__(self, worker: "Worker"):
        self.worker = worker
        self.config = self.worker.config
        self.handle_request = telemetry.trace_function(self.handle_request)
        self.can_handle = telemetry.trace_function(self.can_handle)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "ufys/0.0.0 (https://github.com/jemand771/ufys)"})

    def handle_request(self, req: UfysRequest) -> UfysResponse:
        pass

    def can_handle(self, req: UfysRequest) -> bool:
        if self.regex and self.regex.match(req.url) is None:
            return False
        if self.hostnames and urllib.parse.urlparse(req.url).hostname not in self.hostnames:
            return False
        return True

    def upload_file(
        self, path: Path, hash_: str, meta: UfysResponseMetadata, dim: tuple[int, int]
    ) -> UfysResponse:
        return UfysResponse(
            **dataclasses.asdict(meta),
            video_url=self.worker.reupload(path, hash_),
            width=dim[0],
            height=dim[1],
            reuploaded=True
        )

    @telemetry.trace_function
    def download_file(self, url: str, path: Path):
        with self.session.get(url, stream=True) as r:
            r.raise_for_status()
            with open(path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8 * 1024):
                    f.write(chunk)

    @telemetry.trace_function
    def find_dimensions_from_url(self, url: str):
        with TemporaryDirectory() as _tmp:
            tmp = Path(_tmp)
            file = tmp / "video"
            self.download_file(url, file)
            return self.find_video_dimensions_from_file(file)

    @staticmethod
    @telemetry.trace_function
    def find_video_dimensions_from_file(path: Path):
        streams = ffmpeg.probe(path, select_streams="v").get("streams", [])
        assert len(streams) == 1
        return streams[0].get("width"), streams[0].get("height")

    @telemetry.trace_function
    def convert_video_to_mp4(self, source: Path, dest: Path):
        ffmpeg.input(
            str(source),
            vsync="0"
        ).filter_(
            "scale",
            "trunc(iw/2)*2",
            "trunc(ih/2)*2"
        ).output(
            str(dest),
            pix_fmt="yuv420p",
            movflags="faststart"
        ).run()
