def get_sudo_user_id():
    import pwd, os
    return pwd.getpwnam(os.environ['SUDO_USER']).pw_uid


def get_sudo_group_id():
    import grp, os
    return grp.getgrnam((os.environ['SUDO_USER'])).gr_gid


def ensure_sudo_mode(some_function):
    import os
    import sys
    if sys.platform.startswith("linux") and os.getuid() != 0:
        raise NoPrivilegedMode("\n\nOn Linux platforms, 'docker' is a priviledged command. To use 'docker' functionality, please run in sudo mode or as root user.")
    return some_function


class Log:
    verbose = True

    def __init__(self):
        pass

    @staticmethod
    def write_log(message):
        if Log.verbose:
            print message


class NoPrivilegedMode(Exception):
    pass
