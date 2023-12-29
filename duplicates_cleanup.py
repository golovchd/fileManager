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
CONDITION_LATEST = "latest"
CONDITION_EARLIEST = "earliest"
DATE_GROUP = "date"
ALLOWED_CONDITIONS = [CONDITION_LATEST, CONDITION_EARLIEST]
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

    def check_conditional_rule(
            self, rule: Dict[str, str], dir_a: str, dir_b: str) -> str:
        if not (KEEP_PREFIX in rule and DELETE_PREFIX in rule and
                CONDITION in rule):
            return ""
        if rule[CONDITION] not in ALLOWED_CONDITIONS:
            return ""
        match_left_keep = match(rule[KEEP_PREFIX], dir_a)
        match_right_delete = match(rule[DELETE_PREFIX], dir_b)
        if match_left_keep and match_right_delete:
            if (rule[CONDITION] == CONDITION_EARLIEST and
                    match_left_keep.group(DATE_GROUP) <
                    match_right_delete.group(DATE_GROUP)):
                return LEFT_DIR_KEEP_ACTION
            if (rule[CONDITION] == CONDITION_LATEST and
                    match_left_keep.group(DATE_GROUP) >
                    match_right_delete.group(DATE_GROUP)):
                return LEFT_DIR_KEEP_ACTION
        match_right_keep = match(rule[KEEP_PREFIX], dir_b)
        match_left_delete = match(rule[DELETE_PREFIX], dir_a)
        if match_right_keep and match_left_delete:
            if (rule[CONDITION] == CONDITION_EARLIEST and
                    match_right_keep.group(DATE_GROUP) <
                    match_left_delete.group(DATE_GROUP)):
                return RIGHT_DIR_KEEP_ACTION
            if (rule[CONDITION] == CONDITION_LATEST and
                    match_right_keep.group(DATE_GROUP) >
                    match_left_delete.group(DATE_GROUP)):
                return RIGHT_DIR_KEEP_ACTION
        return ""

    def select_dir_to_keep(self, dir_a: str, dir_b: str) -> str:
        """Returns dir to keep based on rules."""
        matched_rules: List[MatchedRule] = []
        for rule in self.config.get(DIR_CLEANUP_RULES, []):
            rule_result = (self.check_rule_basic(rule, dir_a, dir_b)
                           or self.check_skip_rule(rule, dir_a, dir_b)
                           or self.check_conditional_rule(rule, dir_a, dir_b))
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
