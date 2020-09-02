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
        try:
            sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
            LOG.debug(f"connecting {host}:{port}")
            sock.connect((host, int(port)))
            transport = paramiko.Transport(sock=sock)
        except Exception as e:
            LOG.error(e)
            raise ConnectionError
        try:
            transport.connect(username=user, password=pswd)
        except Exception as e:
            LOG.error(e)
            raise AuthenticationError
        self.connection = paramiko.SFTPClient.from_transport(transport)

    def list_dir(self, base_dir):
        res = self.connection.listdir_attr(base_dir)
        res = [File(i.filename, i.st_mtime, i.st_size, f"{base_dir}/{i.filename}") for i in res]
        return res

    def read_file(self, file_path):
        return self.connection.file(file_path, 'r')