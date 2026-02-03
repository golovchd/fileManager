#!/usr/bin/python3
"""Utils to view/analize file database."""
import argparse
import logging
from pathlib import Path

from file_database import DEFAULT_DATABASE
from file_manager_implementation import (SORT_OPTIONS, SORT_OPTIONS_UNIQUE,
                                         FileUtils)


def list_disks_command(file_db: FileUtils, args: argparse.Namespace) -> int:
    """Listing disks."""
    file_db.list_disks(args.disk, args.size, args.sort)
    return 0


def list_dir_command(file_db: FileUtils, args: argparse.Namespace) -> int:
    """Listing directory."""
    try:
        file_db.list_dir(
            args.disk, args.dir_path, args.recursive, summary=args.summary, print_sha=args.print_sha)
        return 0
    except ValueError:
        print(f"Failed to find dir path {args.dir_path} on drive {args.disk}")
        return 1


def diff_command(file_db: FileUtils, args: argparse.Namespace) -> int:
    """Listing disks."""
    return file_db.diff(args.disk1_path, args.disk2_path)


def find_command(file_db: FileUtils, args: argparse.Namespace) -> int:
    return file_db.find(args.disk, args.dir, args.name, args.include_path, args.exclude_path, args.size, args.print_sha)


def move_command(file_db: FileUtils, args: argparse.Namespace) -> int:
    return file_db.move_fs_item(
            args.disk, args.src_path, args.dst_path, args.dry_run)


def backups_count_command(file_db: FileUtils, args: argparse.Namespace) -> int:
    return file_db.backups_count(args.disk, args.count_limit, args.parent_path)


def update_disk_command(file_db: FileUtils, args: argparse.Namespace) -> int:
    file_db.update_disk(args.disk)
    return 0


def unique_files_command(file_db: FileUtils, args: argparse.Namespace) -> int:
    file_db.unique_files(args.disk, args.sort)
    return 0


def path_redundancy_command(file_db: FileUtils, args: argparse.Namespace) -> int:
    file_db.path_redundancy(args.disks, args.path, args.exclude_path, files_count_limit = args.count_limit)
    return 0


def delete_disk_command(file_db: FileUtils, args: argparse.Namespace) -> int:
    return file_db.delete_disk(args.disk, args.clear_orfan_files, args.force)


def parse_arguments() -> argparse.Namespace:
    """CLI arguments parser."""
    arg_parser = argparse.ArgumentParser(
        description="Utility to view/analize file database.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    arg_parser.add_argument(
        "-v", "--verbose", help="Verbose output", action="store_true")
    arg_parser.add_argument(
        "--database",
        type=Path,
        help="Database file path",
        required=False,
        default=DEFAULT_DATABASE)
    disk_required = ["backups-count", "list-dir", "move", "update-disk", "delete-disk"]
    arg_parser.add_argument(
        "-d", "--disk", type=str,
        help=("Disk label or UUID to process, requirted for " +
              ", ".join(disk_required)))
    subparsers = arg_parser.add_subparsers(
        help="Commands supported by CLI tool", dest="command")

    backups_count = subparsers.add_parser(
        "backups-count", help=("Displays files of the disk(s) with specific "
                               "number of copies"))
    backups_count.set_defaults(func=backups_count_command,
                               cmd_name="backups-count")
    backups_count.add_argument(
        "-c", "--count-limit",
        help="Max number of file's backups to select, default 1",
        type=int, default=1)
    backups_count.add_argument("-p", "--parent-path", type=str,
                               help="Filter files by parent path")

    list_disks = subparsers.add_parser(
        "list-disks", help="List disks with statistic")
    list_disks.set_defaults(func=list_disks_command, cmd_name="list-disks")
    list_disks.add_argument(
        "-s", "--size", help="Calculate space used by files",
        action="store_true")
    list_disks.add_argument(
        "--sort", help="Sort output", choices=SORT_OPTIONS,
        default=SORT_OPTIONS[0])

    list_dir = subparsers.add_parser(
        "list-dir", help="List directory with statistic")
    list_dir.set_defaults(func=list_dir_command, cmd_name="list-dir")
    list_dir.add_argument("dir_path", type=str, help="Path to dir to list")
    list_dir.add_argument(
        "-r", "--recursive", help="List dir recursively", action="store_true")
    output_format = list_dir.add_mutually_exclusive_group()
    output_format.add_argument(
        "-s", "--summary", help="Print only summary", action="store_true")
    output_format.add_argument(
        "-p", "--print-sha", help="Print SHA for each file", action="store_true")

    diff = subparsers.add_parser(
        "diff", help="Diff <disk1>/<path1> vs <disk2>/<path2>")
    diff.set_defaults(func=diff_command, cmd_name="diff")
    diff.add_argument("disk1_path", type=str, help="Path to dir at disk 1")
    diff.add_argument("disk2_path", type=str, help="Path to dir at disk 2")

    find = subparsers.add_parser(
        "find", help="Find file or folder in DB")
    find.set_defaults(func=find_command, cmd_name="find")
    find.add_argument("-n", "--name", type=str, required=True, help="Name matching pattern, could include bash wiledcards ? and *")
    find.add_argument("-d", "--dir", action="store_true", default=False, help="Look for folders, not a files")
    find.add_argument("-i", "--include-path", type=str, nargs="*", help="Include path pattern, could include bash wiledcards ? and *")
    find.add_argument("-e", "--exclude-path", type=str, nargs="*", help="Exclude path pattern, could include bash wiledcards ? and *")
    find.add_argument("-s", "--size", type=str, help="Size filter. In bytes by default, could use K/KB/KiB/.../TiB suffixes and +/- to match bigger and smaller than specified files")
    find.add_argument("-p", "--print-sha", help="Print SHA for each file", action="store_true")

    move_object = subparsers.add_parser(
        "move", help="Move dir, update DB accordingly")
    move_object.set_defaults(func=move_command, cmd_name="move")
    move_object.add_argument("src_path", type=str, help="Path to dir to move")
    move_object.add_argument("dst_path", type=str, help="Destination path")
    move_object.add_argument("--dry-run",
                             help="Do not move dir, run all checks",
                             action="store_true")

    update_disk = subparsers.add_parser(
        "update-disk", help="Update disk with given UUID")
    update_disk.set_defaults(func=update_disk_command, cmd_name="update-disk")

    unique_files = subparsers.add_parser(
        "unique-files", help="Calculate size of unique files")
    unique_files.set_defaults(
        func=unique_files_command, cmd_name="unique-files")
    unique_files.add_argument(
        "--sort", help="Sort output", choices=SORT_OPTIONS_UNIQUE,
        default=SORT_OPTIONS_UNIQUE[0])

    path_redundancy = subparsers.add_parser(
        "path-redundancy", help="Calculate redundancy of specific path")
    path_redundancy.set_defaults(func=path_redundancy_command, cmd_name="path-redundancy")
    path_redundancy.add_argument(
        "--disks", help="Disks to include, at least 2 disks required", type=str, nargs="+", required=True)
    path_redundancy.add_argument(
        "--path", help="Directory path to include", type=str, nargs='*', required=True)
    path_redundancy.add_argument(
        "-e", "--exclude-path", type=str, nargs='*', help="List of path to exclude")
    path_redundancy.add_argument(
        "-c", "--count-limit", type=int, default=1, help="Max number of file's backups to select, default 1")

    delete_disk = subparsers.add_parser(
        "delete-disk", help="Delete disk and file records on it")
    delete_disk.set_defaults(func=delete_disk_command, cmd_name="delete-disk")
    delete_disk.add_argument("-f", "--force",
                             help="Force disk deletion, do not ask confirmation",
                             action="store_true", default=False)
    delete_disk.add_argument("-c", "--clear-orfan-files",
                             help="Clear orfan file records after disk deletion",
                             action="store_true", default=False)

    args = arg_parser.parse_args()
    if args.cmd_name in disk_required and not args.disk:
        arg_parser.error(f"-d DISK argument is required for {args.cmd_name}")

    if args.cmd_name == "path-redundancy" and len(args.disks) < 2:
        arg_parser.error(f"--disks DISK1,DISK2,... argument is require at least 2 disks for {args.cmd_name}")

    return args


def main() -> int:
    """CLI executor."""
    args = parse_arguments()
    lvl = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=lvl, format="%(asctime)s [%(levelname)s] %(message)s")
    logging.debug(args)
    with FileUtils(args.database) as file_db:
        return args.func(file_db, args)


if __name__ == '__main__':
    exit(main())
