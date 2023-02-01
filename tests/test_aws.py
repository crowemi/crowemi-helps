import unittest
import json

from crowemi_helps.aws.aws_s3 import AwsCore, AwsS3


class TestAws(unittest.TestCase):
    def test_core_client(self):
        pass

    def test_s3_get_object(self):
        s3 = AwsS3('us-west-2')
        obj = s3.get_object(key="manifest.json", bucket="crowemi-trades")
        assert obj

    def test_s3_get_object_contents(self):
        s3 = AwsS3('us-west-2')
        obj_contents = s3.get_object_content(bucket="crowemi-trades", key="manifest.json")
        assert obj_contents
        obj = json.loads(obj_contents)
        assert len(obj) >= 1