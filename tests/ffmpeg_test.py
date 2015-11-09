import unittest
import os
import shutil
import sys
import subprocess
import logging
sys.path.append(os.path.join(os.path.dirname(__file__), os.pardir))
from db_drift import Videos, db_session
from settings import FTP_ENTRY

__author__ = 'Hao Lin'


class FfmpegTest(unittest.TestCase):

    video = db_session.query(Videos).filter_by(showvideouuid="048e9908-7155-11e5-8ff7-0026b9414f30").first()

    def test_clip_generation(self):
        log = logging.getLogger("FfmpegTest.test_clip_generation")
        self.assertIsInstance(self.video, Videos)
        video = self.video

        server_folder = '/'.join(video.uri_1200.split("/")[:-1])
        video_path_1200 = os.path.join(os.getcwd(), "temp", video.showvideouuid, video.uri_1200.split("/")[-1])
        video_path_400 = os.path.join(os.getcwd(), "temp", video.showvideouuid, video.uri_400.split("/")[-1])
        lang = video.lang
        self.assertTrue(os.path.isfile(video_path_400))
        command = ["ffprobe", "-v", "error", "-show_entries", "format=duration",
                   "-of", "default=noprint_wrappers=1:nokey=1", video_path_400]
        process = subprocess.Popen(command)
        second_string = process.communicate()[0]
        self.assertIsNone(second_string)
        log.debug("%s", second_string)
        seconds = int(float(str(second_string).strip()))
        self.assertEquals(seconds, 10)
        print seconds

        inpoint = seconds - 11
        outpoint = seconds - 1

        for video_path in [video_path_1200, video_path_400]:
            # Create Videoasset
            destpath = video_path.split('.')[0] + "_10secs.mp4"
            # Generate 10 seconds here
            video_command = ["ffmpeg", "-i", video_path, "-ss", str(inpoint), "-to", str(outpoint), "-an",
                            "-maxrate", "600k", "-bufsize", "1200k", "-profile:v", "baseline", "-level", "3.1",
                            "-f", "mp4", "-movflags", "+faststart", destpath]
            print video_command
            subprocess.Popen(video_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
            print "10 seconds clip generated"

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stderr)
    logging.getLogger("FfmpegTest.test_clip_generation").setLevel(logging.DEBUG)
    unittest.main()
