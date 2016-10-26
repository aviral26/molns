from constants import Constants
import os



class QsubberException(Exception):
    pass


class Qsubber:
    def __init__(self, ip_address,  username, secret_key_file, port=Constants.DEFAULT_QSUB_SSH_PORT):
        if not os.path.exists(secret_key_file):
            raise QsubSubmitNodeException("Cannot access {0}".format(secret_key_file))

        self.ip_address = ip_address
        self.port = port
        self.username = username
        self.secret_key_file = secret_key_file

    def execute_command(self, command):
        raise QsubberException("TODO")
