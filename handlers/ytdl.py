import dataclasses
import re
from pathlib import Path
from tempfile import TemporaryDirectory

from yt_dlp import YoutubeDL
from yt_dlp.utils import DownloadError

import telemetry
import util
from handlers.base import RequestHandler
from model import UfysError, UfysRequest, UfysResponse, UfysResponseMetadata


class YTDLRequestHandler(RequestHandler):
    regex = re.compile(r".*")
    YTDL_OPTS = dict(
        progress_with_newline=True,
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ytdl = YoutubeDL(self.YTDL_OPTS)
        self.ytdl.extract_info = telemetry.trace_function(self.ytdl.extract_info)

    def handle_request(self, req: UfysRequest) -> UfysResponse:
        try:
            info = self.ytdl.extract_info(req.url, download=False)
        except DownloadError:
            raise UfysError("download-error", message="yt-dlp failed to download this video")
        type_ = info.get("_type", "video")
        if type_ == "video":
            return self.handle_video(req, info)
        if type_ == "playlist":
            entries = info.get("entries", [])
            if not entries:
                raise UfysError("empty-playlist", "no video found in playlist")
            if direct_url := entries[0].get("url"):
                return self.handle_direct_url(direct_url, info)
            if orig_url := entries[0].get("original_url"):
                req.url = orig_url
                return self.handle_request(req)
            raise UfysError("unknown-playlist", message="playlist detected, but unable to process")
        raise UfysError(code="unknown-type", message="unknown media type")

    @telemetry.trace_function
    def handle_video(self, req: UfysRequest, info):
        # sort best to worst
        formats = info.get("formats")[::-1]
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
                # _maybe_ split this handler into two - one for direct linking, one for reuploads
                if not (direct_url := fmt.get("url")):
                    continue
                return self.handle_direct_url(direct_url, info)
            except KeyError:
                # incomplete info -> we probably didn't want this anyway
                continue
        return self.reupload_ytdl(req)

    @telemetry.trace_function
    def handle_direct_url(self, url: str, info):
        if not (width := info.get("width")) or not (height := info.get("height")):
            # we don't know the dimensions
            width, height = self.find_dimensions_from_url(url)
        return UfysResponse(
            **dataclasses.asdict(self.meta_from_info(info)),
            video_url=url,
            width=width,
            height=height
        )

    def reupload_ytdl(self, req: UfysRequest):
        # TODO size limit - pass in via request param? (support for external overrides)
        with TemporaryDirectory() as tmp:
            with util.chdir(tmp):
                info = self.ytdl.extract_info(req.url)
            downloads = info.get("requested_downloads", [])
            assert len(downloads) == 1
            path = Path(downloads[0]["filepath"])
            width = downloads[0].get("width")
            height = downloads[0].get("height")
            if width is None or height is None:
                width, height = self.find_video_dimensions_from_file(path)
            return self.upload_file(
                path=path,
                hash_=req.hash,
                meta=self.meta_from_info(info),
                dim=(width, height)
            )

    @staticmethod
    def meta_from_info(info):
        return UfysResponseMetadata(
            title=info.get("title"),
            creator=info.get("uploader"),
            site=extractor if (extractor := info.get("extractor_key")) != "Generic" else None,
        )
