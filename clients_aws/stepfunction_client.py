import boto3
import time
import json
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

        start = self.client.start_execution(
            stateMachineArn=state_machine_arn,
            name=id_exec,
            input=input_data
        )

        execution_arn = start['executionArn']

        while True:
            response = self.client.describe_execution(
                executionArn=execution_arn
            )
            status = response['status']

            if status == 'SUCCEEDED':
                break
            elif status in ['FAILED', 'TIMED_OUT', 'ABORTED']:
                print(f"A execução falhou ou foi interrompida: {status}")
                break

            time.sleep(5)

        response = self.client.describe_execution(
            executionArn=execution_arn
        )
        output = response.get('output', '')
        output = output.replace('\\', '')
        output = output.replace('""', '"')
        output = json.loads(output)

        complete_url = output['body']

        return complete_url
