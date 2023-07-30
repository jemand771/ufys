import json

from bs4 import BeautifulSoup

from handlers.base import RequestHandler
from model import UfysRequest, UfysResponse


class InstagramRequestHandler(RequestHandler):
    hostnames = ["instagram.com", "www.instagram.com"]

    def handle_request(self, req: UfysRequest) -> UfysResponse:
        # TODO error handling error handling error handling
        r = self.session.get(req.url, proxies=dict(http=self.config.PROXY_URL, https=self.config.PROXY_URL))
        r.raise_for_status()
        soup = BeautifulSoup(r.content, "html.parser")
        data: list = json.loads(soup.find("script", dict(type="application/ld+json")).get_text())
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
