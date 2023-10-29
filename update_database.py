#!/usr/bin/python3
"""Load files to database."""

import argparse
import logging
import sys
import time
from pathlib import Path

from file_database import DEFAULT_DATABASE
from file_database_update import FileDatabaseUpdater

REHASH_INTERVAL = 180  # Number of days before re-hash file if not changed


def main(argv):
    """Module as util use wrapper."""
    arg_parser = argparse.ArgumentParser(
        description="Load files to database")
    arg_parser.add_argument("--media", type=Path,
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
    arg_parser.add_argument("-v", "--verbose",
                            help="Print verbose output",
                            action="count", default=0)
    arg_parser.add_argument("-c", "--clear-orfan-files",
                            help="Clear orfan file records",
                            action="store_true", default=False)
    args = arg_parser.parse_args(argv[1:])
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(message)s",
        level=logging.WARNING - 10 * (args.verbose if args.verbose < 3 else 2))

    rehash_time = time.time() - args.rehash_interval * 24 * 3600
    with FileDatabaseUpdater(
          args.database, rehash_time) as file_db:
        file_db.update_dir(args.media, max_depth=args.max_depth)
        file_db.handle_orfans(clear_orfan_files=args.clear_orfan_files)


if __name__ == "__main__":
    main(sys.argv)
