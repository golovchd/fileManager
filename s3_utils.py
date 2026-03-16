from __future__ import annotations

import logging
from time import CLOCK_MONOTONIC, clock_gettime_ns
from typing import Any

from boto3 import client

from storage_client import StorageClient


class S3Client(StorageClient):
    def __init__(self, media: str) -> None:
        self.set_media(media)
        self._client = client("s3")

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
        paginator = self._client.get_paginator("list_objects_v2")
        result = paginator.paginate(Bucket=self._bucket, Prefix=f"{self._prefix}/" if self._prefix else "", Delimiter="/")
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

    def read_file_info(self, file_path: str, get_hash: bool = False) -> tuple[str, str, int, float, str, int]:
        start_time = clock_gettime_ns(CLOCK_MONOTONIC)
        response = self._client.head_object(
                Bucket=self._bucket,
                Key=f"{self._prefix}/{file_path}" if self._prefix else file_path
        )
        file_name_parts = file_path.split(".")
        if len(file_name_parts) > 1 and file_name_parts[0]:
            file_type = file_name_parts[-1]
            file_name = ".".join(file_name_parts[:-1])
        else:
            file_name = file_path
            file_type = ""
        duration = clock_gettime_ns(CLOCK_MONOTONIC) - start_time
        logging.debug(f"read_file_info for s3://{self._bucket}/{self._prefix}/{file_path} in {duration / 1E9:.2f} sec. got {response}")
        return (
            file_name, file_type, response["ContentLength"], response["LastModified"].timestamp(),
            response["ETag"].strip('"'), duration
        )
