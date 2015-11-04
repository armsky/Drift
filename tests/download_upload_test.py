import unittest
import os
import shutil
import sys
sys.path.insert(0, os.path.abspath('../'))
from db_drift import Videos
from settings import FTP_ENTRY


__author__ = 'Hao Lin'


class DUTest(unittest.TestCase):

    test_video = Videos(1, "faking it 220 s2",
                           "mtv",
                           "05324fa0-f840-11e4-98f1-0026b9414f30",
                           "17b3b6b6-554a-11e5-8ff7-0026b9414f30",
                           "65092728-539e-11e5-8ff7-0026b9414f30",
                           "fffc03b0-8a53-11e3-bb6f-0026b9414f30",
                           "/GSPstor/gsp-alias/mediabus/mtv.com/2015/09/03/11/44/850435/5691539_850435_20150903114410885_640x360_1200_m30.mp4",
                           "/GSPstor/gsp-alias/mediabus/mtv.com/2015/09/03/11/44/850435/5691539_850435_20150903114410885_384x216_400_m30.mp4",
                           "eng",
                           "alias")

    def test_download(self):

        # Test MOUNT way
        self.assertIsInstance(self.test_video, Videos)

        # mount point
        mount_point = FTP_ENTRY['alias']['LOCAL_MOUNT']
        self.assertEqual(mount_point, "/GSPstor/gsp-alias")
        self.assertFalse(os.path.isdir(mount_point))

        # shutil copy
        folder_path = "../temp/" + self.test_video.showvideouuid
        print folder_path
        self.assertTrue(os.path.isdir(folder_path))
        local_path_1200 = os.path.join(folder_path, self.test_video.uri_1200.split('/')[-1])
        print local_path_1200
        self.assertRaises(IOError, lambda: shutil.copyfile(self.test_video.uri_1200, local_path_1200))

        # Test FTP way
        # TODO

    def test_upload(self):
        # TODO
        pass

