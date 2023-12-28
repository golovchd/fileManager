from pathlib import Path

from yaml import Loader, load


class DuplicatesCleanup:
    def __init__(self, config_file: Path):
        self.config = load(config_file.read_text("utf-8"), Loader=Loader)
        print(self.config)
