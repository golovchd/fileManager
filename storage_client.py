from __future__ import annotations

from typing import Any


class StorageClient:
    def __init__(self, media: str):
        self._media = media

    @property
    def media(self) -> str:
        return self._media

    def is_symlink(self, path: str = '') -> bool:
        raise NotImplementedError("is_symlink method must be implemented by subclasses")

    def set_media(self, media: str):
        raise NotImplementedError("set_media method must be implemented by subclasses")

    def read_dir(self):
        raise NotImplementedError("read_dir method must be implemented by subclasses")

    def get_disk_info(self) -> dict[str, Any]:
        raise NotImplementedError("get_disk_info method must be implemented by subclasses")

    def get_path_from_mount(self):
        raise NotImplementedError("get_path_from_mount method must be implemented by subclasses")

    def read_file_info(self, file_path: str, get_hash: bool = False):
        raise NotImplementedError("read_file_info method must be implemented by subclasses")
