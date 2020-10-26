import paramiko
import loguru
from structures import File
import socket

LOG = loguru.logger


class ConnectionError(Exception):
    pass


class AuthenticationError(Exception):
    pass


class FTP:

    def __init__(self, host, port, user, pswd, **kwargs):
        self.client = paramiko.SSHClient()
        self.client.load_system_host_keys()
        self.host = host
        self.port = port
        self.user = user
        self.pswd = pswd
        self.kwargs =kwargs
        self.connect()

    def connect(self):
        try:
            LOG.debug(f"connecting {self.host}:{self.port}")
            if "ipv6" in self.kwargs and self.kwargs.get("ipv6"):
                sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
                sock.connect((self.host, int(self.port)))
                self.transport = paramiko.Transport(sock=sock)
            else:
                self.transport = paramiko.Transport(self.host, int(self.port))
        except Exception as e:
            LOG.error(e)
            raise ConnectionError
        try:
            self.transport.connect(username=self.user, password=self.pswd)
        except Exception as e:
            LOG.error(e)
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
