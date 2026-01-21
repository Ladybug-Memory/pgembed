from pathlib import Path
import typing
from typing import Optional, List, Dict
import subprocess
import json
import logging
import hashlib
import socket
import platform
import stat
import psutil
import datetime
import shutil

_logger = logging.getLogger('pgembed')

class PostmasterInfo:
    """Struct with contents of the PGDATA/postmaster.pid file, contains information about the running server.
    Example of file contents: (comments added for clarity)
    cat /Users/orm/Library/Application Support/Postgres/var-15/postmaster.pid
        ```
        3072        # pid
        /Users/orm/Library/Application Support/Postgres/var-15 # pgdata
        1712346200  # start_time
        5432    # port
        /tmp # socker_dir, where .s.PGSQL.5432 is located
        localhost # listening on this hostname
        8826964     65536 # shared mem size?, shmget id (can deallocate with sysv_ipc.remove_shared_memory(shmget_id))
        ready # server status
        ```
    """

    def __init__(self, lines : List[str]):
        _lines = ['pid', 'pgdata', 'start_time', 'port', 'socket_dir', 'hostname', 'shared_memory_info', 'status']
        assert len(lines) == len(_lines), f"_lines: {_lines=} lines: {lines=}"
        clean_lines = [ line.strip() for line in lines ]

        raw : Dict[str,str] = dict(zip(_lines, clean_lines))

        self.pid = int(raw['pid'])
        self.pgdata = Path(raw['pgdata'])
        self.start_time = datetime.datetime.fromtimestamp(int(raw['start_time']))

        if raw['socket_dir']:
            self.socket_dir = Path(raw['socket_dir'])
        else:
            self.socket_dir = None

        if raw['hostname']:
            self.hostname = raw['hostname']
        else:
            self.hostname = None

        if raw['port']:
            self.port = int(raw['port'])
        else:
            self.port = None

        # not sure what this is in windows
        self.shmem_info = raw['shared_memory_info']
        self.status = raw['status']

        self.process = None # will be not None if process is running
        self._init_process_meta()

    def _init_process_meta(self) -> Optional[psutil.Process]:
        if self.pid is None:
            return
        try:
            process = psutil.Process(self.pid)
        except psutil.NoSuchProcess:
            return

        self.process = process
        # exact_create_time = datetime.datetime.fromtimestamp(process.create_time())
        # if abs(self.start_time - exact_create_time) <= datetime.timedelta(seconds=1):

    def is_running(self) -> bool:
        return self.process is not None and self.process.is_running()

    @classmethod
    def read_from_pgdata(cls, pgdata : Path) -> Optional['PostmasterInfo']:
        postmaster_file = pgdata / 'postmaster.pid'
        if not postmaster_file.exists():
            return None

        lines = postmaster_file.read_text().splitlines()
        return cls(lines)

    def get_uri(self, user : str = 'postgres', database : Optional[str] = None) -> str:
        """ Returns a connection uri string for the postgresql server using the information in postmaster.pid"""
        if database is None:
            database = user

        if self.socket_dir is not None:
            return f"postgresql://{user}:@/{database}?host={self.socket_dir}"
        elif self.port is not None:
            assert self.hostname is not None
            return f"postgresql://{user}:@{self.hostname}:{self.port}/{database}"
        else:
            raise RuntimeError("postmaster.pid does not contain port or socket information")

    @property
    def shmget_id(self) -> Optional[int]:
        if platform.system() == 'Windows':
            return None

        if not self.shmem_info:
            return None
        raw_id = self.shmem_info.split()[-1]
        return int(raw_id)

    @property
    def socket_path(self) -> Optional[Path]:
        if self.socket_dir is not None:
            # TODO: is the port always 5432 for the socket? or does it depend on the port in postmaster.pid?
            return self.socket_dir / f'.s.PGSQL.{self.port}'
        return None

    def __repr__(self) -> str:
        return f"PostmasterInfo(pid={self.pid}, pgdata={self.pgdata}, start_time={self.start_time}, hostname={self.hostname} port={self.port}, socket_dir={self.socket_dir} status={self.status}, process={self.process})"

    def __str__(self) -> str:
        return self.__repr__()

def process_is_running(pid : int) -> bool:
    assert pid is not None
    return psutil.pid_exists(pid)

if platform.system() != 'Windows':
    def ensure_user_exists(username : str) -> Optional['pwd.struct_passwd']:
        """ Ensure system user `username` exists.
            Returns their pwentry if user exists, otherwise it creates a user through `useradd`.
            Assume permissions to add users, eg run as root.
        """
        import pwd

        try:
            entry = pwd.getpwnam(username)
        except KeyError:
            entry = None

        if entry is None:
            subprocess.run(["useradd", "-s", "/bin/bash", username], check=True, capture_output=True, text=True)
            entry = pwd.getpwnam(username)

        return entry

    def ensure_prefix_permissions(path: Path):
        """ Ensure target user can traverse prefix to path
            Permissions for everyone will be increased to ensure traversal.
        """
        # ensure path exists and user exists
        assert path.exists()
        prefix = path.parent
        # chmod g+rx,o+rx: enable other users to traverse prefix folders
        g_rx_o_rx = stat.S_IRGRP |  stat.S_IROTH | stat.S_IXGRP | stat.S_IXOTH
        while True:
            curr_permissions = prefix.stat().st_mode
            ensure_permissions = curr_permissions | g_rx_o_rx
            # TODO: are symlinks handled ok here?
            prefix.chmod(ensure_permissions)
            if prefix == prefix.parent: # reached file system root
                break
            prefix = prefix.parent

    def ensure_folder_permissions(path: Path, flag : int):
        """ Ensure target user can read,  and execute the folder.
            Permissions for everyone will be increased to ensure traversal.
        """
        # read and traverse folder
        g_rx_o_rx = stat.S_IRGRP |  stat.S_IROTH | stat.S_IXGRP | stat.S_IXOTH

        def _helper(path: Path):
            if path.is_dir():
                path.chmod(path.stat().st_mode | g_rx_o_rx )
                for child in path.iterdir():
                    _helper(child)
            else:
                path.chmod(path.stat().st_mode | flag)

        _helper(path)

class DiskList:
    """ A list of integers stored in a file on disk.
    """
    def __init__(self, path : Path):
        self.path = path

    def get_and_add(self, value : int) -> List[int]:
        old_values = self.get()
        values = old_values.copy()
        if value not in values:
            values.append(value)
            self.put(values)
        return old_values

    def get_and_remove(self, value : int) -> List[int]:
        old_values = self.get()
        values = old_values.copy()
        if value in values:
            values.remove(value)
            self.put(values)
        return old_values

    def get(self) -> List[int]:
        if not self.path.exists():
            return []
        return json.loads(self.path.read_text())

    def put(self, values : List[int]) -> None:
        self.path.write_text(json.dumps(values))


def socket_name_length_ok(socket_name : Path):
    ''' checks whether a socket path is too long for domain sockets
        on this system. Returns True if the socket path is ok, False if it is too long.
    '''
    if socket_name.exists():
        return socket_name.is_socket()

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.bind(str(socket_name))
        return True
    except OSError as err:
        if 'AF_UNIX path too long' in str(err):
            return False
        raise err
    finally:
        sock.close()
        socket_name.unlink(missing_ok=True)

def find_suitable_socket_dir(pgdata, runtime_path) -> Path:
    """ Assumes server is not running. Returns a suitable directory for used as pg_ctl -o '-k ' option.
        Usually, this is the same directory as the pgdata directory.
        However, if the pgdata directory exceeds the maximum length for domain sockets on this system,
        a different directory will be used.
    """
    # find a suitable directory for the domain socket
    # 1. pgdata. simplest approach, but can be too long for unix socket depending on the path
    # 2. runtime_path. This is a directory that is intended for storing runtime data.

    # for shared folders, use a hash of the path to avoid collisions of different folders
    # use a hash of the pgdata path combined with inode number to avoid collisions
    string_identifier = f'{pgdata}-{pgdata.stat().st_ino}'
    path_hash = hashlib.sha256(string_identifier.encode()).hexdigest()[:10]

    candidate_socket_dir = [
        pgdata,
        runtime_path / path_hash,
    ]

    ok_path = None
    for path in candidate_socket_dir:
        path.mkdir(parents=True, exist_ok=True)
        # name used by postgresql for domain socket is .s.PGSQL.5432
        if socket_name_length_ok(path / '.s.PGSQL.5432'):
            ok_path = path
            _logger.info(f"Using socket path: {path}")
            break
        else:
            _logger.info(f"Socket path too long: {path}. Will try a different directory for socket.")

    if ok_path is None:
        raise RuntimeError("Could not find a suitable socket path")

    return ok_path

def find_suitable_port(address : Optional[str] = None) -> int:
    """Find an available TCP port."""
    if address is None:
        address = '127.0.0.1'
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((address, 0))
    port = sock.getsockname()[1]
    sock.close()
    return port
