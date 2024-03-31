import unittest

import main


class TestWeb(unittest.TestCase):

    def setUp(self):
        self.app = main.APP.test_client()

    # TODO add flask tests
