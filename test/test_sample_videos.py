import unittest

import yt_dlp.utils

import constants


class TestSampleVideos(unittest.TestCase):

    def setUp(self):
        self.ytdl = yt_dlp.YoutubeDL()

    def test_available(self):
        for url in (
            constants.video_direct_linkable,
            constants.video_needs_reupload,
        ):
            with self.subTest(url):
                result = self.ytdl.extract_info(url, download=False)
                self.assertIsInstance(result, dict)

    def test_not_available(self):
        for url in (
            constants.video_not_found,
        ):
            with self.subTest(url):
                self.assertRaises(
                    yt_dlp.utils.DownloadError,
                    self.ytdl.extract_info, url, download=False
                )
