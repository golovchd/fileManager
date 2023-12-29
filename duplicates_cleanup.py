from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

from regex import match
from yaml import Loader, load

DIR_CLEANUP_RULES = "dir-commons"
KEEP_PREFIX = "keep"
DELETE_PREFIX = "delete"
SKIP_PREFIX = "skip"
CONDITION = "condition"
LEFT_DIR_KEEP_ACTION = "l"
RIGHT_DIR_KEEP_ACTION = "r"
SKIP_ACTION = "s"
ALLOWED_ACTIONS = [LEFT_DIR_KEEP_ACTION, RIGHT_DIR_KEEP_ACTION, SKIP_ACTION]


@dataclass
class MatchedRule:
    rule: Dict[str, str]
    action: str


class DuplicatesCleanup:
    def __init__(self, config_file: Path):
        if config_file and config_file.exists():
            self.config = load(config_file.read_text("utf-8"), Loader=Loader)
        else:
            self.config = {}

    def check_rule_basic(self, rule: Dict[str, str], dir_a: str, dir_b: str
                         ) -> str:
        if not (KEEP_PREFIX in rule and DELETE_PREFIX in rule and
                CONDITION not in rule):
            return ""
        if (match(rule[KEEP_PREFIX], dir_a)
                and match(rule[DELETE_PREFIX], dir_b)):
            return LEFT_DIR_KEEP_ACTION
        if (match(rule[KEEP_PREFIX], dir_b)
                and match(rule[DELETE_PREFIX], dir_a)):
            return RIGHT_DIR_KEEP_ACTION
        return ""

    def check_skip_rule(self, rule: Dict[str, str], dir_a: str, dir_b: str
                        ) -> str:
        if not (KEEP_PREFIX in rule and SKIP_PREFIX in rule and
                CONDITION not in rule):
            return ""
        if (match(rule[KEEP_PREFIX], dir_a)
                and match(rule[SKIP_PREFIX], dir_b)):
            return SKIP_ACTION
        if (match(rule[KEEP_PREFIX], dir_b)
                and match(rule[SKIP_PREFIX], dir_a)):
            return SKIP_ACTION
        return ""

    def select_dir_to_keep(self, dir_a: str, dir_b: str) -> str:
        """Returns dir to keep based on rules."""
        matched_rules: List[MatchedRule] = []
        for rule in self.config.get(DIR_CLEANUP_RULES, []):
            rule_result = (self.check_rule_basic(rule, dir_a, dir_b)
                           or self.check_skip_rule(rule, dir_a, dir_b))
            if rule_result:
                matched_rules.append(MatchedRule(rule, rule_result))
        print(matched_rules)
        if not matched_rules:
            return SKIP_ACTION
        matched_action = matched_rules[0].action
        print(f"matched_action={matched_action}")
        for matched in matched_rules:
            if matched.action != matched_action:
                return SKIP_ACTION
        return matched_action
