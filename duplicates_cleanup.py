from pathlib import Path
from typing import Dict

from yaml import Loader, load

DIR_CLEANUP_RULES = "dir-commons"
KEEP_PREFIX = "keep"
DELETE_PREFIX = "delete"
LEFT_DIR_KEEP_ACTION = "l"
RIGHT_DIR_KEEP_ACTION = "r"
SKIP_ACTION = "s"
ALLOWED_ACTIONS = [LEFT_DIR_KEEP_ACTION, RIGHT_DIR_KEEP_ACTION, SKIP_ACTION]


class DuplicatesCleanup:
    def __init__(self, config_file: Path):
        if config_file and config_file.exists():
            self.config = load(config_file.read_text("utf-8"), Loader=Loader)
        else:
            self.config = {}

    def check_rule_basic(self, rule: Dict[str, str], dir_a: str, dir_b: str
                         ) -> str:
        if not (KEEP_PREFIX in rule and DELETE_PREFIX in rule):
            return SKIP_ACTION
        if (dir_a.startswith(rule[KEEP_PREFIX])
                and dir_b.startswith(rule[DELETE_PREFIX])):
            return LEFT_DIR_KEEP_ACTION
        if (dir_b.startswith(rule[KEEP_PREFIX])
                and dir_a.startswith(rule[DELETE_PREFIX])):
            return RIGHT_DIR_KEEP_ACTION
        return SKIP_ACTION

    def select_dir_to_keep(self, dir_a: str, dir_b: str) -> str:
        """Returns dir to keep based on rules."""
        for rule in self.config.get(DIR_CLEANUP_RULES, []):
            basic_result = self.check_rule_basic(rule, dir_a, dir_b)
            if basic_result:
                return basic_result
        return SKIP_ACTION
