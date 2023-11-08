import json

from bs4 import BeautifulSoup
from opentelemetry import trace

from handlers.base import RequestHandler
from model import UfysError, UfysRequest, UfysResponse


class InstagramRequestHandler(RequestHandler):
    hostnames = ["instagram.com", "www.instagram.com"]

    def handle_request(self, req: UfysRequest) -> UfysResponse:
        # TODO error handling error handling error handling
        r = self.session.get(req.url, proxies=dict(http=self.config.PROXY_URL, https=self.config.PROXY_URL))
        trace.get_current_span().add_event(
            "downloaded-html",
            dict(
                content=r.content,
                status_code=r.status_code
            )
        )
        r.raise_for_status()
        soup = BeautifulSoup(r.content, "html.parser")
        data_element = soup.find("script", dict(type="application/ld+json"))
        if not data_element:
            raise UfysError(code="no-data-element", message="instagram didn't send a video data element")
        data: list = json.loads(data_element.get_text())
        social_media_posting = next(x for x in data if x.get("@type") == "SocialMediaPosting")
        video, *_ = social_media_posting.get("video")
        author = social_media_posting.get("author")
        author_str = f"{author.get('name')} ({author.get('alternateName')})"
        title, *_ = video.get("caption").split("\n")

        return UfysResponse(
            title=title,
            creator=author_str,
            site="Instagram",
            video_url=video.get("contentUrl"),
            width=int(video.get("width")),
            height=int(video.get("height"))
        )
