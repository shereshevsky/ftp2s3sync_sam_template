import paramiko
import loguru
from structures import File
import socket
import retry

LOG = loguru.logger


class ConnectionError(Exception):
    pass


class AuthenticationError(Exception):
    pass


class FTP:

    def __init__(self, host, port, user, pswd, **kwargs):
        self.client = paramiko.SSHClient()
        self.client.load_system_host_keys()
        self.connection = self.connect(host, kwargs, port, pswd, user)

    @retry.retry(tries=5, delay=20, logger=LOG)
    def connect(self, host, kwargs, port, pswd, user):
        try:
            LOG.debug(f"connecting {host}:{port}")
            if "ipv6" in kwargs and kwargs.get("ipv6"):
                sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
                sock.connect((host, int(port)))
                transport = paramiko.Transport(sock=sock)
            else:
                transport = paramiko.Transport(host, int(port))
        except Exception as e:
            LOG.error(e)
            raise ConnectionError
        try:
            transport.connect(username=user, password=pswd)
        except Exception as e:
            LOG.error(e)
            raise AuthenticationError
        return paramiko.SFTPClient.from_transport(transport)

    def list_dir(self, base_dir):
        res = self.connection.listdir_attr(base_dir)
        res = [File(i.filename, i.st_mtime, i.st_size, f"{base_dir}/{i.filename}") for i in res if i.filename.endswith('.txt') or i.filename.endswith('.zip')]
        return res

    def read_file(self, file_path):
        return self.connection.file(file_path, 'r')
