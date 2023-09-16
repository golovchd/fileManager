"""File utils module."""

import hashlib
import logging
import os
import subprocess
import sys


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


def read_file(file_path):
    """Returns name, type, size, mtime, sha1 of file."""
    file_size = os.path.getsize(file_path)
    mtime = os.path.getmtime(file_path)
    file_name = os.path.basename(file_path)
    file_name_parts = file_name.split('.')
    if len(file_name_parts) > 1:
        file_type = file_name_parts[-1]
        file_name = '.'.join(file_name_parts[:-1])
    else:
        file_type = ''
    sha1_hash = hashlib.sha1()
    with open(file_path, 'rb') as file_handler:
        sha1_hash.update(file_handler.read())
    return file_name, file_type, file_size, mtime, sha1_hash.hexdigest()
