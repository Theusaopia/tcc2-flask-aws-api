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

    def save_rdf_to_bucket(self, id_exec, rdf_file, ontology_file, mapping_file):
        folder = f"{self.folder}{id_exec}/"
        self.client.upload_file(
            rdf_file, self.bucket_name, folder + rdf_file)

        self.client.upload_file(
            ontology_file, self.bucket_name, folder + ontology_file)

        self.client.upload_file(
            mapping_file, self.bucket_name, folder + mapping_file)
