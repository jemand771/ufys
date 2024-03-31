import os
import time

import docker
import requests
import unittest
from minio import Minio

import constants
import worker
from model import UfysRequest


class TestWorker(unittest.TestCase):

    def setUp(self):

        minio_endpoint = self.run_service("minio/minio", "9000/tcp", "server /data")
        Minio(minio_endpoint, "minioadmin", "minioadmin", secure=False).make_bucket("test")
        self.worker = worker.Worker(worker.ConfigStore(
            MINIO_BUCKET="test",
            MINIO_ENDPOINT=minio_endpoint,
            MINIO_SECURE=False,
            MINIO_ACCESS_KEY="minioadmin",
            MINIO_SECRET_KEY="minioadmin"
        ))

    def run_service(self, image: str, port: str, command: str = None, **kwargs):
        client = docker.from_env()
        container = client.containers.run(
            image=image,
            command=command,
            ports={
                port: None
            },
            **kwargs,
            detach=True,
            auto_remove=True
        )
        for _ in range(30):
            container.reload()
            if container.status == "running":
                break
            time.sleep(0.1)
        else:
            container.kill()
            self.skipTest(f"failed to start image {image}")
        self.addCleanup(container.kill)
        port_bindings = container.attrs["NetworkSettings"]["Ports"]
        if not (binding := port_bindings.get(port)):
            self.skipTest(f"failed to get port binding for {port} for {image} container")
        port = int(binding[0]["HostPort"])
        return f"localhost:{port}"

    # TODO find new sample video
    @unittest.skip("direct linking broken on tiktok")
    def test_direct_extraction(self):
        resp = self.worker.handle_request(UfysRequest(url=constants.video_direct_linkable))
        self.assertEqual(1, len(resp))
        video, = resp
        print(video)
        self.assertEqual("jackmanifoldtv", video.creator)
        self.assertEqual("#duet with @Juney  am i doing it right?", video.title)
        self.assertEqual(False, video.reuploaded)
        self.assertEqual(1216, video.width)
        self.assertEqual(1080, video.height)
        r = requests.get(video.video_url)
        r.raise_for_status()

    @unittest.skipIf(os.environ.get("GITHUB_ACTIONS"), "github actions is banned from reddit")
    def test_minio_reupload(self):
        self.worker.handle_request(UfysRequest(url=constants.video_needs_reupload))
        objects = self.worker.minio.list_objects(self.worker.config.MINIO_BUCKET)
        self.assertEqual(len(list(objects)), 1)
