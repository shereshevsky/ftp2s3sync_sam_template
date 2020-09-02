import boto3
from typing import List, Dict


class SSM:
    def __init__(self):
        self.ssm = boto3.client('ssm')

    def list_namespace(self, path: str) -> List[Dict[str, str]]:
        res = self.ssm.get_parameters_by_path(Path=path,
                                              Recursive=False,
                                              WithDecryption=True,
                                              )
        return [{"path": i.get("Name"), "connection_string": i.get("Value")} for i in res.get("Parameters")]

    def get_secret(self, name: str) -> str:
        response = self.ssm.get_parameter(
            Name=name,
            WithDecryption=True
        )
        return response['Parameter']['Value']

