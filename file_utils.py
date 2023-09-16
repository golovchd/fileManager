"""File utils module."""

import hashlib
import logging
import os
import subprocess
import sys
from pathlib import Path
from time import CLOCK_MONOTONIC, clock_gettime_ns
from typing import Tuple


def get_full_dir_path(path):
    """Converting path to absolute path to dir."""
    abs_path = os.path.abspath(os.path.expanduser(path))
    if not os.path.isdir(abs_path):
        abs_path = os.path.dirname(abs_path)
    return abs_path


def get_mount_path(dir_path):
    """Returning path of mount point for given dir."""
    logging.debug('get_mount_path for %s', dir_path)
    if os.path.ismount(dir_path):
        logging.debug('mount_path %s', dir_path)
        return dir_path
    return get_mount_path(os.path.dirname(dir_path))


def get_path_from_mount(dir_path):
    """Returning list dir from mount point to current."""
    mount_path = get_mount_path(dir_path)
    if mount_path == '/':
        local_path = dir_path
    else:
        local_path = dir_path.replace(get_mount_path(dir_path), '')
    return local_path.split('/')[1:]


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


def read_dir(dir_path):
    """Reading details of files and subdirs."""
    for _, dirs, files in os.walk(dir_path):
        return files, dirs


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


def read_file(file_path: Path) -> Tuple[str, str, int, float, str]:
    """Returns name, type, size, mtime, sha1 of file."""
    file_stat = file_path.stat()
    file_name = file_path.name
    file_name_parts = file_name.split('.')
    if len(file_name_parts) > 1:
        file_type = file_name_parts[-1]
        file_name = '.'.join(file_name_parts[:-1])
    else:
        file_type = ''
    sha1_hex = generate_file_sha1(file_path)
    return (
        file_name, file_type, file_stat.st_size, file_stat.st_mtime, sha1_hex
    )
