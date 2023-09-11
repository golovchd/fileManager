#!/usr/bin/python3
"""Load files to database."""

import argparse

import file_database


def main():
    """Module as util use wrapper."""
    args = argparse.ArgumentParser(
        description='Load files to database')
    args.add_argument('--media', help='Media to import files from',
                      default=None)
    args.add_argument('--max_depth',
                      help=('Number of levels below media to process, '
                            '0 for single level'),
                      default=None)
    args.add_argument('--verbose',
                      help='Print verbose output',
                      action="store_true", default=False)
    args.add_argument('--dry_run',
                      help='Print action instead of executing it',
                      action="store_true", default=False)
    args.parse_args(namespace=args)

    with file_database.FileManagerDatabase() as file_db:
        file_db.update_dir(args.media, max_depth=args.max_depth)


if __name__ == '__main__':
    main()
