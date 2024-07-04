from handlers.base import RequestHandler
from model import UfysRequest, UfysResponse


class InstagramRequestHandler(RequestHandler):
    hostnames = ["instagram.com", "www.instagram.com"]

    def handle_request(self, req: UfysRequest) -> UfysResponse:
        r = self.session.get("https://i.instagram.com/api/v1/oembed/", params=dict(
            url=req.url
        ))
        r.raise_for_status()
        meta = r.json()

        r = self.session.post(
            "https://api.cobalt.tools/api/json",
            json=dict(url=req.url),
            headers=dict(Accept="application/json")
        )
        r.raise_for_status()
        video_url = r.json()["url"]
        width, height = self.find_dimensions_from_url(video_url)

        return UfysResponse(
            title=meta["title"],
            creator=meta["author_name"],
            site="Instagram",
            video_url=video_url,
            width=width,
            height=height
        )
