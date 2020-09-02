import boto3
import json
from typing import List, Dict, Union, AnyStr


class SSM:
    def __init__(self):
        self.ssm = boto3.client('ssm')

    def list_namespace(self, path: AnyStr) -> List[Dict[AnyStr, Union[AnyStr, Dict]]]:
        # res = self.ssm.get_parameters_by_path(Path=path,
        #                                       Recursive=False,
        #                                       WithDecryption=True,
        #                                       )
        # return [{"path": i.get("Name"),
        #          "connection_string": json.loads(i.get("Value"))} for i in res.get("Parameters")]

        return [{"s3_path": "GDB",
                 "connection_parameters": {"bucket": "dwhmain",
                                           "ftp_dir": "GDB/download/GDB_Data_Synchronization/GDB_Data",
                                           "host": "2605:97c0:2050:101f:1::1",
                                           "port": "22",
                                           "user": "LA-Region",
                                           "pswd": "m@QSQ6r43x"
                                           }}
                ]
