from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from yaml import Loader, load

from file_manager.file_utils import convert_to_bytes


class MediaType(Enum):
    PHOTOS = 1
    VIDEOS = 2
    DASHCAM = 3


@dataclass
class MediaConfig:
    name: str
    label: re.Pattern
    types: list[MediaType]


class ImportConfig:
    def __init__(self, config_file: Path) -> None:
        self.config = load(config_file.read_text("utf-8"), Loader=Loader)
        logging.debug("ImportConfig: %s", self.config)

    @property
    def storage_regex_list(self) -> list[str]:
        return self.config.get("storage-config", {}).get(
                "storage-includes", ["^not-a-disk$"])

    @property
    def import_roots_list(self) -> list[str]:
        logging.debug("import_roots_list: %s", self.config.get("storage-config", {}).get("import-locations", {}).values())
        return list({import_location["root"] for import_location in self.config.get("storage-config", {}).get("import-locations", {}).values()})

    @property
    def free_space_limits(self) -> dict[str, int]:
        if "free-space-limit" not in self.config.get("storage-config", {}):
            return {}
        free_space_limits = {}
        free_space_config = self.config["storage-config"]["free-space-limit"]
        if "percentage" in free_space_config:
            free_space_limits["percentage"] = int(
                    free_space_config["percentage"])
        if "absolute" in free_space_config:
            free_space_limits["absolute"] = convert_to_bytes(
                    free_space_config["absolute"])

        return free_space_limits

    @property
    def media_config(self) -> dict[str, MediaConfig]:
        if "media-config" not in self.config:
            return {}
        return {
            details["label"]: MediaConfig(details["name"], re.compile(details["label"]),
                                          [MediaType[str(media_type).upper()]
                                           for media_type in details["types"]])
            for details in self.config["media-config"]
        }
