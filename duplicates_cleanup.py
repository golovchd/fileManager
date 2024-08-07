import logging
from dataclasses import dataclass
from pathlib import Path
from re import match
from typing import Dict, List

from yaml import Loader, load

DIR_CLEANUP_RULES = "dir-commons"
FILE_CLEANUP_RULES = "same-files"
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
        logging.debug(f"select_dir_to_keep({dir_a}, {dir_b})")
        matched_rules: List[MatchedRule] = []
        for rule in self.config.get(DIR_CLEANUP_RULES, []):
            rule_result = (self.check_rule_basic(rule, dir_a, dir_b)
                           or self.check_skip_rule(rule, dir_a, dir_b)
                           or self.check_conditional_rule(rule, dir_a, dir_b))
            if rule_result:
                logging.debug(f"Rule {rule} matched pair {dir_a} "
                              f"{dir_b} as {rule_result}")
                matched_rules.append(MatchedRule(rule, rule_result))
        if not matched_rules:
            return SKIP_ACTION
        matched_action = matched_rules[0].action
        for matched in matched_rules:
            if matched.action != matched_action:
                return SKIP_ACTION
        return matched_action

    def select_file_to_keep(self, names: Dict[int, str]) -> int:
        for rule in self.config.get(FILE_CLEANUP_RULES, []):
            keep_index = 0
            delete_indexes: List[int] = []
            group_values = {group_name: "" for group_name in rule["groups"]}
            for index, name in names.items():
                if not index:
                    raise IndexError(
                        "Names index could not be 0 for select_file_to_keep")
                keep_match = match(rule[KEEP_PREFIX], name)
                delete_match = match(rule[DELETE_PREFIX], name)
                if not (keep_match or delete_match):
                    break  # none of rules match
                if keep_index and keep_match and not delete_match:
                    break  # more than one matched rule
                for group_name in rule["groups"]:
                    if keep_match and not delete_match:
                        matched_value = keep_match.group(group_name)
                    elif delete_match:
                        matched_value = delete_match.group(group_name)
                    else:
                        raise ValueError("Both keep_match and delete_match "
                                         f"missing for {name}")
                    if not group_values[group_name]:
                        group_values[group_name] = matched_value
                    else:
                        if group_values[group_name] != matched_value:
                            keep_index = 0  # Reset to skip if rule mismatched
                            delete_indexes = []
                            break
                else:  # No groups mismatch
                    if keep_match and not delete_match:
                        logging.debug(f"Matched: keep rule {rule[KEEP_PREFIX]}"
                                      f" for {index}: {name}")
                        keep_index = index
                    else:
                        logging.debug(f"Matched: del rule {rule[KEEP_PREFIX]}"
                                      f" for {index}: {name}")
                        delete_indexes.append(index)
                    continue  # Check next file
                break  # Stop testing rule that had mismatched group
            for idx in names:
                if idx != keep_index and idx not in delete_indexes:
                    break  # Each index should match either keep or delete rule
            else:
                return keep_index
        return 0
