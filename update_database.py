#!/usr/bin/python3
"""Load files to database."""

import argparse
import logging
import sys
from pathlib import Path

import file_database


def main(argv):
    """Module as util use wrapper."""
    arg_parser = argparse.ArgumentParser(
        description='Load files to database')
    arg_parser.add_argument('--media', help='Media to import files from',
                            default=None)
    arg_parser.add_argument(
        '--max_depth',
        type=int,
        help='Number of levels below media to process, 0 for single level',
        default=None)
    arg_parser.add_argument(
        '--database',
        type=Path,
        help='Database file',
        required=False,
        default=file_database.DEFAULT_DATABASE_NAME)
    arg_parser.add_argument('-v', '--verbose',
                            help='Print verbose output',
                            action='count', default=0)
    arg_parser.add_argument('--dry_run',
                            help='Print action instead of executing it',
                            action="store_true", default=False)
    args = arg_parser.parse_args(argv[1:])
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(message)s",
        level=logging.WARNING - 10 * (args.verbose if args.verbose < 3 else 2))

    with file_database.FileManagerDatabase(db_name=args.database) as file_db:
        file_db.update_dir(args.media, max_depth=args.max_depth)


if __name__ == '__main__':
    main(sys.argv)
