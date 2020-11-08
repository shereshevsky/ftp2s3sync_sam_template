import paramiko
from structures import File
import socket
from ftplib import FTP as oFTP
from io import BytesIO


class ConnectionError(Exception):
    pass


class AuthenticationError(Exception):
    pass


class OldFTP:
    def __init__(self, host, port, user, pswd, logger, **kwargs):
        self.host = host
        self.port = port
        self.user = user
        self.pswd = pswd
        self.kwargs =kwargs
        self.logger = logger
        self.client = self.connect()

    def connect(self):
        self.client = oFTP(self.host)
        self.client.login(self.user, self.pswd)

    def list_dir(self, base_dir):
        res = []
        for n, attr in self.client.mlsd(base_dir):
            if n.endswith(".zip"):
                res.append(File(n, int(attr["modify"]), int(attr["size"]), f"{base_dir}/{n}"))
        return res

    def read_file(self, file_path):
        self.connect()
        buffer = BytesIO()
        self.client.retrbinary(f"RETR {file_path.split('/')[-1]}", buffer.write)
        buffer.seek(0)
        return buffer


class FTP:

    def __init__(self, host, port, user, pswd, logger, **kwargs):
        self.client = paramiko.SSHClient()
        self.client.load_system_host_keys()
        self.host = host
        self.port = port
        self.user = user
        self.pswd = pswd
        self.kwargs =kwargs
        self.logger = logger
        self.connect()

    def connect(self):
        try:
            self.logger.info(f"connecting {self.host}:{self.port}")
            if "ipv6" in self.kwargs and self.kwargs.get("ipv6"):
                sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
                sock.connect((self.host, int(self.port)))
                self.transport = paramiko.Transport(sock=sock)
            else:
                self.transport = paramiko.Transport(self.host, int(self.port))
        except Exception as e:
            self.logger.error(e)
            raise ConnectionError
        try:
            self.transport.connect(username=self.user, password=self.pswd)
        except Exception as e:
            self.logger.error(e)
            raise AuthenticationError
        self.connection = paramiko.SFTPClient.from_transport(self.transport)

    def list_dir(self, base_dir):
        res = self.connection.listdir_attr(base_dir)
        res = [File(i.filename, i.st_mtime, i.st_size, f"{base_dir}/{i.filename}")
               for i in res if i.filename.endswith(".zip")]
        return res

    def read_file(self, file_path):
        self.connect()
        return self.connection.file(file_path, 'r')
