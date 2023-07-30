import pathlib
import urllib.parse
from tempfile import TemporaryDirectory

from bs4 import BeautifulSoup

import telemetry
from handlers.base import RequestHandler
from model import UfysError, UfysRequest, UfysResponse, UfysResponseMetadata


class AsciinemaRequestHandler(RequestHandler):
    hostnames = ["asciinema.org"]

    def handle_request(self, req: UfysRequest) -> UfysResponse:
        id_, = urllib.parse.urlparse(req.url).path.removeprefix("/a/").split("/")
        if self.config.AAAS_ENDPOINT is None:
            raise UfysError(code="config-error", message="AAAS_ENDPOINT not set")
        r = self.session.get(f"https://asciinema.org/a/{id_}.cast?dl=1")
        r.raise_for_status()
        r = self.session.post(self.config.AAAS_ENDPOINT, data=r.content)
        r.raise_for_status()
        with TemporaryDirectory() as _tmp:
            gif = pathlib.Path(_tmp) / "render.gif"
            mp4 = pathlib.Path(_tmp) / "render.mp4"
            with open(gif, "wb") as file:
                file.write(r.content)
            self.convert_video_to_mp4(source=gif, dest=mp4)
            return self.upload_file(
                path=mp4,
                hash_=req.hash,
                dim=self.find_video_dimensions_from_file(mp4),
                meta=self.scrape_metadata(id_=id_)
            )

    @telemetry.trace_function
    def scrape_metadata(self, id_) -> UfysResponseMetadata:
        # TODO marie is looking into an api for this
        r = self.session.get(f"https://asciinema.org/a/{id_}")
        r.raise_for_status()
        soup = BeautifulSoup(r.content, "html.parser")
        title = soup.find("meta", dict(property="og:title")).attrs.get("content")
        user_href = soup.find("span", {"class": "author-avatar"}).find("a").attrs.get("href")
        r = self.session.get(f"https://asciinema.org{user_href}")
        r.raise_for_status()
        soup = BeautifulSoup(r.content, "html.parser")
        user_string = soup.find("h1").find(string=True, recursive=False).get_text().strip()
        return UfysResponseMetadata(
            title=title,
            creator=user_string,
            site="asciinema"
        )
