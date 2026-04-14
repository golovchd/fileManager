from __future__ import annotations

from file_manager.file_utils import FsClient
from file_manager.s3_utils import S3Client
from file_manager.storage_client import StorageClient


def get_storage_client(media: str) -> StorageClient:
    if media.startswith("s3://"):
        return S3Client(media)
    return FsClient(media)
