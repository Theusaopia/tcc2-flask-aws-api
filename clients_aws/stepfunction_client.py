import boto3
from utils.constant import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY


class StepFunctionClient():

    def __init__(self) -> None:
        self.client = boto3.client('stepfunctions',
                                   region_name='sa-east-1',
                                   aws_access_key_id=AWS_ACCESS_KEY_ID,
                                   aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    def start_step_function(self, id_exec):
        input_data = '{"id_exec": "' + id_exec + '"}'
        state_machine_arn = 'arn:aws:states:sa-east-1:745684883488:stateMachine:fluxo-inicia-instancia'

        response = self.client.start_execution(
            stateMachineArn=state_machine_arn,
            name=id_exec,
            input=input_data
        )

        return response['executionArn']
