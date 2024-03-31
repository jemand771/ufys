import unittest


class MinioConfig:
    PORT = "9000/tcp"
    container = None

    @staticmethod
    def fail():
        raise unittest.SkipTest("failed to set up minio")

    def __enter__(self):
        return w

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.container.kill()
