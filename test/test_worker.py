import unittest

import requests

from . import constants
from .. import worker
from ..model import UfysRequest


class TestWorker(unittest.TestCase):

    def test_direct_extraction(self):
        wk = worker.Worker()
        resp = wk.handle_request(UfysRequest(url=constants.video_direct_linkable))
        self.assertEqual(resp.creator, "dracosweeney")
        self.assertEqual(resp.title, "Gotta make sure the memory card is in place 💀 ")
        self.assertEqual(resp.reuploaded, False)
        self.assertEqual(resp.width, 720)
        self.assertEqual(resp.height, 1280)
        r = requests.get(resp.video_url)
        r.raise_for_status()

    def test_minio_reupload(self):
        wk = worker.Worker(
            worker.ConfigStore(
                MINIO_BUCKET="test",
                MINIO_ENDPOINT="localhost:9080",
                MINIO_SECURE=False,
                MINIO_ACCESS_KEY="minioadmin",
                MINIO_SECRET_KEY="minioadmin"
            )
        )
        wk.handle_request(UfysRequest(url=constants.video_needs_reupload))
        objects = wk.minio.list_objects(wk.config.MINIO_BUCKET)
        self.assertEqual(len(list(objects)), 1)
