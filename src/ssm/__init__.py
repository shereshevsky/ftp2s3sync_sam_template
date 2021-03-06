import boto3
import json
from typing import List, Dict, Union, AnyStr
from settings import *


class SSM:
    def __init__(self):
        if AWS_CONFIG:
            self.ssm = boto3.client('ssm', config=AWS_CONFIG)
        else:
            self.ssm = boto3.client('ssm')

    def list_namespace(self, path: AnyStr) -> List[Dict[AnyStr, Union[AnyStr, Dict]]]:
        res = self.ssm.get_parameters_by_path(Path=path,
                                              Recursive=False,
                                              WithDecryption=True,
                                              )
        return [{"s3_path": i.get("Name"),
                 "connection_parameters": json.loads(i.get("Value"))} for i in res.get("Parameters")]
