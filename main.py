#!/usr/bin/python3
import argparse
import hashlib
import logging
import os
import stat
from pathlib import Path


class fileDetails:
    def __init__(self, path, name, useContent=False):
        self.path = path
        self.name = name
        self.stat = Path(path / name).stat()
        if useContent:
            self.sha1 = hashlib.sha1(Path(path / name).read_bytes)
        else:
            self.sha1 = hashlib.sha1(name + str(self.stat))
            # logging.info(f"File: {path}\{name} size:{self.stat.st_size}");

# class folderDetails:
#     def __init__(self):


class folderProcessor:
    def __init__(self, useHash, printVerbose):
        self.useHash = useHash
        self.printVerbose = printVerbose

    def process(self, Location, checkType=True):
        file_list = []
        name_ref = {}
        name_size_ref = {}
        content_ref = {}
        if checkType:
            locationStat = Path(Location).stat()
            logging.info(f"Location {args.Location} is dir: "
                         f"{stat.S_ISDIR(locationStat.st_mode)}")
        else:
            logging.warning(f"Skip type check for Location {args.Location}\n")
        if not checkType or stat.S_ISDIR(locationStat.st_mode):
            for dirpath, _, filenames in os.walk(Location):
                for file in filenames:
                    file_list.append(fileDetails(dirpath, file, self.useHash))
                    idx = len(file_list) - 1
                    if file in name_ref:
                        name_ref[file].append(idx)
                        logging.info(f"Entry info:\t{dirpath}\\{file} "
                                     f"size:{file_list[idx].stat.st_size}")
                        logging.info(f"\tduplicates by name with: "
                                     f"{len(name_ref[file])} files")
                    else:
                        name_ref[file] = [idx]
                    name_size = f"{file}_{file_list[idx].stat.st_size}"
                    if name_size in name_size_ref:
                        name_size_ref[name_size].append(idx)
                        logging.info(f"Entry info:\t{dirpath}\\{file} "
                                     f"size:{file_list[idx].stat.st_size}")
                        logging.info(f"\tduplicates by name/size with: "
                                     f"{len(name_size_ref[name_size])} files")
                    else:
                        name_size_ref[name_size] = [idx]
                    if self.useHash:
                        sha1 = file_list[idx].sha1
                        if sha1 in content_ref:
                            content_ref[sha1].append(idx)
                            logging.info(f"Entry info:\t{dirpath}\\{file} "
                                         f"size:{file_list[idx].stat.st_size}")
                            logging.info(f"\tduplicates by content with: "
                                         f"{len(content_ref[sha1])} files")
                        else:
                            content_ref[sha1] = [idx]


parser = argparse.ArgumentParser(description='Managing files')

parser.add_argument('Location', metavar='Location', type=str,
                    help='Location to be processed')
parser.add_argument('--hash', action='store_true', default=False,
                    help="Calculate file's content hash")
parser.add_argument('-v', action='store_true', default=False,
                    help='Verbose output')

args = parser.parse_args()

fp = folderProcessor(args.hash, args.v)

try:
    fp.process(args.Location)
except OSError as e:
    logging.error(
        f"Error while processing location {args.Location}:\n\t{str(e)}")
else:
    logging.warning(f"Finished processing location {args.Location}")
