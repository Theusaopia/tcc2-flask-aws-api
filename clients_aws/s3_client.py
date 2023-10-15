import boto3
from utils.constant import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY


class S3Client():

    def __init__(self) -> None:
        self.client = boto3.client('s3',
                                   region_name='sa-east-1',
                                   aws_access_key_id=AWS_ACCESS_KEY_ID,
                                   aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        self.bucket_name = 'bucket-rdf'
        self.folder = 'files/'

    def save_rdf_to_bucket(self, filename):
        self.client.upload_file(
            filename, self.bucket_name, self.folder + filename)
