import boto3
import json
from typing import List, Dict, Union, AnyStr


class SSM:
    def __init__(self):
        self.ssm = boto3.client('ssm')

    def list_namespace(self, path: AnyStr) -> List[Dict[AnyStr, Union[AnyStr, Dict]]]:
        res = self.ssm.get_parameters_by_path(Path=path,
                                              Recursive=False,
                                              WithDecryption=True,
                                              )
        return [{"s3_path": i.get("Name"),
                 "connection_parameters": json.loads(i.get("Value"))} for i in res.get("Parameters")]
