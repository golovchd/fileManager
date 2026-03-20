from __future__ import annotations

import sys

from storage_client import StorageClient


def get_storage_client(media: str) -> StorageClient:
    if media.startswith("s3://"):
        if "s3_utils" not in sys.modules:
            from s3_utils import S3Client
        return S3Client(media)

    if "file_utils" not in sys.modules:
        from file_utils import FsClient
    return FsClient(media)
