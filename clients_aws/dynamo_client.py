import boto3
from utils.constant import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY


class DynamoClient():

    def __init__(self) -> None:
        self.client = boto3.client('dynamodb',
                                   region_name='sa-east-1',
                                   aws_access_key_id=AWS_ACCESS_KEY_ID,
                                   aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        self.table_name = 'controle_processamento'

    def insert_control_data(self, id_execucao, status):
        self.client.put_item(
            TableName=self.table_name,
            Item={
                'id_execucao': {'S': id_execucao},
                'status': {'S': status}
            }
        )
