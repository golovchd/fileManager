"""File utils module."""

from __future__ import annotations

import hashlib
import json
import logging
import subprocess
import sys
from hashlib import md5
from pathlib import Path
from time import CLOCK_MONOTONIC, clock_gettime_ns
from typing import Any

from storage_client import StorageClient

_IGNORED_DIRS = [".", "..", "$RECYCLE.BIN"]

PARTSIZES_DEFAULTS : list[int] = [ ## Default Partsizes Map (bytes)
  8388608, # aws_cli/boto3
  15728640, # s3cmd
]


def get_full_dir_path(path: Path) -> Path:
    """Converting path to absolute path to dir."""
    abs_path = path.expanduser().resolve()
    if abs_path.is_file():
        return abs_path.parent
    return abs_path


def get_mount_path(dir_path: Path) -> Path:
    """Returning path of mount point for given dir."""
    logging.debug(f"get_mount_path for {dir_path}")
    while not dir_path.is_mount():
        dir_path = dir_path.parent
    logging.debug(f"mount_path {dir_path}")
    return dir_path


def get_path_from_mount(dir_path: Path) -> list[str]:
    """Returning list dir from mount point to current."""
    relative_from_mount = dir_path.relative_to(get_mount_path(dir_path))
    if relative_from_mount == Path("."):
        return [""]
    return str(relative_from_mount).split("/")


def get_disk_info(uuid: str) -> dict[str, Any]:
    for device_info in get_lsblk():
        if device_info["uuid"] == uuid:
            return device_info
    raise ValueError(f"Failed to locate device for UUID {uuid}")


def get_lsblk() -> list[dict[str, str]]:
    uuid_cmd = ["lsblk", "-a", "--output=UUID,LABEL,SIZE,MOUNTPOINT",
                "--json", "--bytes"]
    lsblk_info = json.loads(subprocess.check_output(uuid_cmd).decode(
                             sys.stdin.encoding))
    logging.debug(lsblk_info)
    return lsblk_info["blockdevices"]


def get_path_disk_info(dir_path: Path) -> dict[str, Any]:
    """Getting disk info for given path."""
    mount_path = str(get_mount_path(dir_path))
    for device_info in get_lsblk():
        if device_info["mountpoint"] == mount_path:
            return {
                "uuid": device_info["uuid"],
                "size": int(device_info.get("fssize", device_info.get("size", 0))) // 1024,
                "label": device_info["label"] or "",
            }
    raise ValueError(f"Failed to locate device for path {dir_path}")


def generate_file_sha1(
        file_path: Path, blocksize: int = 2**20) -> tuple[str, int]:
    """Safe way to get SHA1 for big files."""
    sha1_hash = hashlib.sha1()
    start_time = clock_gettime_ns(CLOCK_MONOTONIC)
    try:
        with open(file_path, 'rb') as file_handler:
            while True:
                buffer = file_handler.read(blocksize)
                if not buffer:
                    break
                sha1_hash.update(buffer)
    except PermissionError:
        logging.warning(
                f"generate_file_sha1 missing permission to read {file_path}")
        return "", 0
    except OSError:
        logging.exception(f"generate_file_sha1 failed to read {file_path}")
        return "", 0
    file_size = file_path.stat().st_size
    duration = clock_gettime_ns(CLOCK_MONOTONIC) - start_time
    mb_per_second = (file_size * 1E3) / duration
    logging.debug(f"{file_path} size {file_size / 1E6:.2f} MB "
                  f"process time {duration / 1E9:.2f} sec. "
                  f"SHA1 hashing speed {mb_per_second:.2f} MB/sec.")
    return sha1_hash.hexdigest(), duration


def read_file(
        file_path: Path, get_sha1: bool
        ) -> tuple[str, str, int, float, str, int]:
    """Returns name, type, size, mtime, sha1 of file."""
    file_stat = file_path.stat()
    file_name = file_path.name
    file_name_parts = file_name.split(".")
    if len(file_name_parts) > 1 and file_name_parts[0]:
        file_type = file_name_parts[-1]
        file_name = ".".join(file_name_parts[:-1])
    else:
        file_type = ""
    sha1_hex, hash_time = "", 0
    if get_sha1:
        sha1_hex, hash_time = generate_file_sha1(file_path)
    return (
        file_name, file_type, file_stat.st_size, file_stat.st_mtime,
        sha1_hex, hash_time
    )

def get_confirmation(message: str, accepted_choices: list[str]) -> bool:
    return input(message) in accepted_choices


def calc_etag(inputfile: Path, partsize: int) -> str:
  md5_digests = []
  with open(inputfile, 'rb') as f:
    for chunk in iter(lambda: f.read(partsize), b''):
      md5_digests.append(md5(chunk).digest())
  return md5(b''.join(md5_digests)).hexdigest() + '-' + str(len(md5_digests))


def factor_of_1MB(filesize: int, num_parts: int) -> int:
  x = filesize / int(num_parts)
  y = x % 1048576
  return int(x + 1048576 - y)


def check_etag(file_path: Path, etag: str) -> bool:
    """Checks if ETag of given file matches provided ETag."""
    file_size = file_path.stat().st_size
    num_parts = int(etag.split('-')[1])
    logging.debug(f"check_etag for {file_path} with size {file_size} and etag {etag}.")
    return etag == calc_etag(file_path, factor_of_1MB(file_size, num_parts))


def get_possible_etags(file_path: Path) -> list[str]:
    """Returns possible ETags for given file based on its size and default partsizes."""
    return [
        calc_etag(file_path, partsize) for partsize in PARTSIZES_DEFAULTS
    ]


class FsClient(StorageClient):
    def __init__(self, media: str) -> None:
        super().__init__(media)
        self.cur_path = get_full_dir_path(Path(media))
        disk_info = get_path_disk_info(self.cur_path)
        self._disk_uuid = disk_info["uuid"]
        self._disk_size = disk_info["size"]
        self._disk_label = disk_info["label"]
        self.mountpoint = get_mount_path(self.cur_path)

    def get_disk_info(self) -> dict[str, Any]:
        return {
            "uuid": self._disk_uuid,
            "size": self._disk_size,
            "label": self._disk_label,
        }

    @property
    def media(self) -> str:
        return str(self.cur_path)

    @property
    def slow_file_read(self) -> bool:
        return False

    @property
    def disk_name(self) -> str:
        return self._disk_label or self._disk_uuid or ""

    def is_symlink(self, path: str = '') -> bool:
        return (self.cur_path / path).is_symlink() if path else self.cur_path.is_symlink()

    def set_media(self, media: str) -> None:
        self._media = media
        self.cur_path = get_full_dir_path(Path(media))

    def get_path_from_mount(self) -> list[str]:
        return get_path_from_mount(self.cur_path)

    def read_dir(self) -> tuple[list[str], list[str]]:
        """Reading details of files and subdirs."""
        try:
            files = sorted([file.name for file in self.cur_path.iterdir()
                            if file.is_file() and not file.is_symlink()])
            dirs = sorted([dir.name for dir in self.cur_path.iterdir()
                        if dir.is_dir() and not dir.is_symlink() and
                        dir.name not in _IGNORED_DIRS])
            logging.debug(f"read_dir({self.cur_path}) dirs: {dirs}, files: {files}")
            return files, dirs
        except PermissionError:
            logging.warning(f"read_dir missing permission to read {self.cur_path}")
            return [], []

    def read_file_info(self, file_path: str, get_hash: bool = False) -> tuple[str, str, int, float, str, int]:
        return read_file(self.cur_path / file_path, get_sha1=get_hash)
