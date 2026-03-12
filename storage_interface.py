from __future__ import annotations

from file_utils import FsClient
from s3_utils import S3Client
from storage_client import StorageClient


def get_storage_client(media: str) -> StorageClient:
    if media.startswith("s3://"):
        return S3Client(media)
    return FsClient(media)
