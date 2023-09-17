"""File utils module."""

import hashlib
import logging
import subprocess
import sys
from pathlib import Path
from time import CLOCK_MONOTONIC, clock_gettime_ns
from typing import List, Tuple


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


def get_path_from_mount(dir_path: Path) -> List[str]:
    """Returning list dir from mount point to current."""
    relative_from_mount = dir_path.relative_to(get_mount_path(dir_path))
    return str(relative_from_mount).split('/')


def get_path_disk_info(dir_path):
    """Getting disk info for given path."""
    df_device_cmd = ['df', dir_path, '--output=source,size']
    device_path = subprocess.check_output(df_device_cmd).decode(
        sys.stdin.encoding).split("\n")[-2].split(" ")
    uuid_cmd = ["lsblk", device_path[0], "--output=UUID,LABEL"]
    device_info = subprocess.check_output(uuid_cmd).decode(
        sys.stdin.encoding).split("\n")[-2].split(" ")
    logging.debug(device_info)
    return {
        "uuid": device_info[0],
        "size": int(device_path[-1]),
        "label": device_info[-1] if len(device_info) > 1 else "",
    }


def read_dir(dir_path: Path) -> Tuple[List[str], List[str]]:
    """Reading details of files and subdirs."""
    try:
        files = [file.name for file in dir_path.iterdir() if file.is_file()]
        dirs = [dir.name for dir in dir_path.iterdir() if dir.is_dir()]
        return files, dirs
    except PermissionError:
        logging.exception(f"read_dir failed to read {dir_path}")
        return [], []


def generate_file_sha1(file_path: Path, blocksize: int = 2**20) -> str:
    """Safe way to get SHA1 for big files."""
    sha1_hash = hashlib.sha1()
    start_time = clock_gettime_ns(CLOCK_MONOTONIC)
    with open(file_path, 'rb') as file_handler:
        while True:
            buffer = file_handler.read(blocksize)
            if not buffer:
                break
            sha1_hash.update(buffer)
    file_size = file_path.stat().st_size
    duration = clock_gettime_ns(CLOCK_MONOTONIC) - start_time
    mb_per_second = (file_size * 1000) / duration
    logging.debug(f"{file_path} size {(file_size / 1000000):.2f} MB "
                  f"process time {(duration / 1000000000):.2f} sec. "
                  f"SHA1 hashing speed {mb_per_second:.2f} MB/sec.")
    return sha1_hash.hexdigest()


def read_file(
        file_path: Path, get_sha1: bool) -> Tuple[str, str, int, float, str]:
    """Returns name, type, size, mtime, sha1 of file."""
    file_stat = file_path.stat()
    file_name = file_path.name
    file_name_parts = file_name.split('.')
    if len(file_name_parts) > 1:
        file_type = file_name_parts[-1]
        file_name = '.'.join(file_name_parts[:-1])
    else:
        file_type = ''
    sha1_hex = generate_file_sha1(file_path) if get_sha1 else ""
    return (
        file_name, file_type, file_stat.st_size, file_stat.st_mtime, sha1_hex
    )
