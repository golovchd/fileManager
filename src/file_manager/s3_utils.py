from __future__ import annotations

import logging
from functools import wraps
from time import CLOCK_MONOTONIC, clock_gettime_ns, sleep
from typing import Any, Callable

import boto3  # type: ignore
from botocore.exceptions import ClientError  # type: ignore

from file_manager.storage_client import StorageClient

_CLIENT_RETRY_TIMEOUT = 30  # seconds


class PermissionError(ClientError):
    pass


def retry_decorator(func: Callable) -> Callable:
    """Decorator to retry a function on ClientError, with a new client instance.
    The function must have a 'client' keyword argument which is an instance of S3Client.
    """
    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        _retry_count = 0
        while True:
            try:
                return func(*args, **kwargs)
            except ClientError as error:
                if error.response['ResponseMetadata']['HTTPStatusCode'] == 403:
                    raise PermissionError(error.response, error.operation_name) from error
                _retry_count += 1
                logging.warning(f"{func.__name__} failed for with args {args}, {kwargs} due to:\n{error}\nRetry {_retry_count} with new client...")
                kwargs["client"].set_client()
                if _retry_count > 1:
                    sleep(_CLIENT_RETRY_TIMEOUT)
    return wrapper


@retry_decorator
def read_dir(client: S3Client) -> tuple[list[str], list[str]]:
    paginator = client.session_client.get_paginator("list_objects_v2")
    result = paginator.paginate(Bucket=client._bucket, Prefix=f"{client._prefix}/" if client._prefix else "", Delimiter="/")
    files = []
    dirs = []
    page_count = 0
    for page in result:
        for dir in page.get("CommonPrefixes", []):
            dirs.append(dir["Prefix"].split("/")[-2])
        for obj in page.get("Contents", []):
            path_parts = obj["Key"].split("/")
            files.append(path_parts[-1])
        page_count += 1

    return files, dirs


@retry_decorator
def read_file_info(client: S3Client, key: str) -> Any:
    return client.session_client.head_object(Bucket=client._bucket, Key=key)


@retry_decorator
def read_file_attributes(client: S3Client, key: str) -> Any:
    return client.session_client.get_object_attributes(Bucket=client._bucket, Key=key, ObjectAttributes=["ETag", "ObjectSize", "StorageClass"])


class S3Client(StorageClient):
    def __init__(self, media: str) -> None:
        self.set_media(media)
        self.set_client()

    def set_client(self):
        new_session = boto3.Session()
        credentials = new_session.get_credentials()
        self.session_client = new_session.client("s3")
        logging.info(f"Created new S3 client for bucket {self._bucket} with access key {credentials.access_key}")

    def is_symlink(self, path: str = '') -> bool:
        del path
        return False

    def set_media(self, media: str) -> None:
        if media.startswith("s3://"):
            media = media[5:]
        path_parts = media.rstrip("/").split("/", 1)
        self._bucket = path_parts[0]
        self._prefix = path_parts[1] if len(path_parts) > 1 else ""
        self._media = self._bucket
        if self._prefix:
            self._media += "/" + self._prefix

    def get_disk_info(self) -> dict[str, Any]:
        return {"uuid": self._bucket, "size": 1, "label": self._bucket}

    @property
    def slow_file_read(self) -> bool:
        return True

    @property
    def disk_name(self) -> str:
        return self._bucket

    def get_path_from_mount(self) -> list[str]:
        return self._prefix.split("/") if self._prefix else [""]

    def read_dir(self) -> tuple[list[str], list[str]]:
        try:
            return read_dir(client=self)
        except PermissionError as error:
            logging.error(f"Permission error while reading directory for s3://{self._bucket}/{self._prefix}: {error}")
            return [], []

    def read_file_info(self, file_path: str, get_hash: bool = False) -> tuple[str, str, int, float, str, int]:
        start_time = clock_gettime_ns(CLOCK_MONOTONIC)
        file_name_parts = file_path.split(".")
        if len(file_name_parts) > 1 and file_name_parts[0]:
            file_type = file_name_parts[-1]
            file_name = ".".join(file_name_parts[:-1])
        else:
            file_name = file_path
            file_type = ""
        try:
            response = read_file_info(client=self, key=f"{self._prefix}/{file_path}" if self._prefix else file_path)
        except PermissionError as error:
            logging.error(f"Permission error while reading file info for s3://{self._bucket}/{self._prefix}: {error}")
            try:
                response = read_file_attributes(client=self, key=f"{self._prefix}/{file_path}" if self._prefix else file_path)
                duration = clock_gettime_ns(CLOCK_MONOTONIC) - start_time
                return (
                    file_name, file_type, response["ObjectSize"], response["LastModified"].timestamp(),
                    response["ETag"].strip('"'), duration
                )
            except PermissionError as error:
                duration = clock_gettime_ns(CLOCK_MONOTONIC) - start_time
                logging.error(f"Permission error while reading file attributes for s3://{self._bucket}/{self._prefix}: {error}")
                return file_name, file_type, 0, 0.0, "unknown-no-access", duration

        duration = clock_gettime_ns(CLOCK_MONOTONIC) - start_time
        logging.debug(f"read_file_info for s3://{self._bucket}/{self._prefix}/{file_path} in {duration / 1E9:.2f} sec. got {response}")
        return (
            file_name, file_type, response["ContentLength"], response["LastModified"].timestamp(),
            response["ETag"].strip('"'), duration
        )
