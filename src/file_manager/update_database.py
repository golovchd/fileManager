#!/usr/bin/python3
"""Load files to database."""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

from file_manager.file_database import DEFAULT_DATABASE
from file_manager.file_database_update import FileDatabaseUpdater
from file_manager.storage_interface import get_storage_client

REHASH_INTERVAL = 180  # Number of days before re-hash file if not changed
DEFAULT_MAX_FILE_UPDATE_THREADS = 4


def main(argv: Any=[]) -> None:
    """Module as util use wrapper."""
    arg_parser = argparse.ArgumentParser(
        description="Load files to database")
    arg_parser.add_argument("--media", type=str,
                            help="Media to import files from",
                            default=None)
    arg_parser.add_argument(
        "--max_depth",
        type=int,
        help="Number of levels below media to process, 0 for single level",
        default=None)
    arg_parser.add_argument(
        "--rehash_interval",
        type=int,
        help=("Number of days before re-hash file if not changed, "
              f"default is {REHASH_INTERVAL}"),
        default=REHASH_INTERVAL)
    arg_parser.add_argument(
        "--database",
        type=Path,
        help="Database file",
        required=False,
        default=DEFAULT_DATABASE)
    os_cpu_count = os.cpu_count()
    default_threads = os_cpu_count // 4 if os_cpu_count else DEFAULT_MAX_FILE_UPDATE_THREADS
    arg_parser.add_argument("-t", "--threads",
                            help=f"Run file reading/hashing in multiple threads, specify number of threads or use -t without value to use default 1/4 of CPU cores ({default_threads})",
                            type=int, nargs="?", const=default_threads, default=1)
    arg_parser.add_argument("-v", "--verbose",
                            help="Print verbose output",
                            action="count", default=0)
    arg_parser.add_argument("-c", "--clear-orfan-files",
                            help="Clear orfan file records",
                            action="store_true", default=False)
    args = arg_parser.parse_args(argv[1:] if argv else sys.argv[1:])
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(message)s",
        level=logging.WARNING - 10 * (args.verbose if args.verbose < 3 else 2))

    rehash_time = time.time() - args.rehash_interval * 24 * 3600
    with FileDatabaseUpdater(
          args.database, rehash_time, args.threads, get_storage_client(args.media)) as file_db:
        file_db.update_dir(max_depth=args.max_depth)
        file_db.handle_orfans(clear_orfan_files=args.clear_orfan_files)


if __name__ == "__main__":
    main()
