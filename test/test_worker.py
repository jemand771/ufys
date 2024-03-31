import requests
import unittest

import constants
import worker
from model import UfysRequest


class TestWorker(unittest.TestCase):

    def test_direct_extraction(self):
        wk = worker.Worker()
        resp = wk.handle_request(UfysRequest(url=constants.video_direct_linkable))
        self.assertEqual(1, len(resp))
        video, = resp
        self.assertEqual("jackmanifoldtv", video.creator)
        self.assertEqual("#duet with @Juney  am i doing it right?", video.title)
        self.assertEqual(False, video.reuploaded)
        self.assertEqual(1216, video.width)
        self.assertEqual(1080, video.height)
        r = requests.get(video.video_url)
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
