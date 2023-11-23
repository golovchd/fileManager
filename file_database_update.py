"""File database update module."""

import logging
from pathlib import Path
from time import CLOCK_MONOTONIC, clock_gettime_ns
from typing import List, Optional, Tuple

import file_utils
from file_database import FileManagerDatabase

_MAX_ORFAN_SEARCH_DEPTH = 256


class FileDatabaseUpdater(FileManagerDatabase):
    def update_file(self, file_full_name: str) -> Tuple[int, int, int]:
        """Updates or inserts in DB details on file.
            Returns hashed size, file size, hash time in ns."""
        if not self._cur_dir_id:
            raise ValueError("Missing _cur_dir_id")
        (
            fsrecord_id, sha1_read_date, db_file_mtime, file_id, db_file_size,
            db_file_sha1
        ) = self.get_db_file_info(file_full_name)
        file_path = self._cur_dir_path / file_full_name
        file_name, file_type, size, mtime, _, _ = file_utils.read_file(
                file_path, False)
        if (self._rehash_time < sha1_read_date and
                db_file_size == size and db_file_mtime == mtime):
            return 0, size, 0   # Current file details matching DB, no re-hash

        file_name, file_type, size, mtime, sha1, hash_time = (
            file_utils.read_file(file_path, True))
        if not sha1:
            logging.warning(f"Failed to get SHA1 for {file_full_name}, "
                            f"SHA1 in DB {db_file_sha1}, skipped update of "
                            f"fsrecord_id={fsrecord_id}")
            return 0, size, 0   # Skip file if unable to read

        new_file_id = self.select_update_file_record(
            sha1, mtime, size, file_name, file_type
        )
        self.update_fsrecord(fsrecord_id, file_full_name, mtime, new_file_id)
        # TODO: Handle deletion of old `files` record file_id
        del file_id
        return size, size, hash_time

    def update_files(self, files: List[str]) -> Tuple[int, int, int]:
        """Updating files in current dir.
            Returns hashed size, file size, hash time in ns."""
        hashed_size = 0
        total_size = 0
        total_hash_time = 0
        for file_name in files:
            if (self._cur_dir_path / file_name).is_symlink():
                logging.warning("update_files called for symlink %s",
                                self._cur_dir_path / file_name)
                continue
            hashsed, size, hash_time = self.update_file(file_name)
            hashed_size += hashsed
            total_size += size
            total_hash_time += hash_time
        return hashed_size, total_size, total_hash_time

    def print_statistic(
                self, path: Path, start_time_ns: int,
                files_count: int, files_hashed_size: int,
                files_total_size: int, files_hash_time_ns: int,
            ) -> None:
        """Calculates and prints dir processing statistic."""
        process_time_ns = clock_gettime_ns(CLOCK_MONOTONIC) - start_time_ns
        average_process_speed = (files_total_size * 1E3) / process_time_ns
        if files_hash_time_ns:
            average_hash_time = (files_hashed_size * 1E3) / files_hash_time_ns
        else:
            average_hash_time = 0
        hashing_size_pct = (100 * files_hashed_size / files_total_size
                            if files_total_size else 0)
        hashing_time_pct = 100 * files_hash_time_ns / process_time_ns
        logging.info(f"Processed {path} in {process_time_ns / 1E9:.2f} sec, "
                     f"{files_count} files, "
                     f"total size {files_total_size / 1E6:.2f} MB, "
                     f"{average_process_speed:.2f} MB/sec.")
        logging.info(f"In {path} hashed {hashing_size_pct:.1f}% by size, "
                     f"{hashing_time_pct:.1f}% by time, "
                     f"{files_hashed_size / 1E6:.2f} MB in "
                     f"{files_hash_time_ns / 1E9:.2f} sec, "
                     f"{average_hash_time:.2f} MB/sec.")

    def update_dir(
            self, path: Path,
            max_depth: Optional[int] = 0, check_disk: bool = True
            ) -> Tuple[int, int, int, int]:
        """Updating DB with dir details, entrypoint for update_database."""
        dir_path = file_utils.get_full_dir_path(path)
        start_time = clock_gettime_ns(CLOCK_MONOTONIC)
        logging.debug(f"update_dir for: {path}, full path: {dir_path}")
        if check_disk:
            self.set_disk(**file_utils.get_path_disk_info(dir_path))
        self.set_cur_dir(dir_path)
        files, sub_dirs = file_utils.read_dir(dir_path)
        files_count = len(files)
        logging.info("update_dir: %s, %d files, %d sub dirs",
                     dir_path, len(files), len(sub_dirs))
        self.clean_cur_dir(files, is_files=True)
        self.clean_cur_dir(sub_dirs, is_files=False)
        files_hashed_size, files_total_size, files_hash_time_ns = (
            self.update_files(files))

        if max_depth != 0:
            for sub_dir_name in sub_dirs:
                if (path / sub_dir_name).is_symlink():
                    logging.warning("update_dir called for symlink %s",
                                    self._cur_dir_path / sub_dir_name)
                    continue
                dir_files_count, dir_hashed_size, dir_size, hash_time_ns = (
                    self.update_dir(
                        path / sub_dir_name,
                        max_depth=max_depth - 1 if max_depth else None,
                        check_disk=False))
                files_count += dir_files_count
                files_hashed_size += dir_hashed_size
                files_total_size += dir_size
                files_hash_time_ns += hash_time_ns
        self.print_statistic(path, start_time, files_count, files_hashed_size,
                             files_total_size, files_hash_time_ns)
        return (
            files_count, files_hashed_size,
            files_total_size, files_hash_time_ns
        )

    def handle_orfans(self, clear_orfan_files: bool = False):
        """Removes fsrecords and file orfans."""
        for i in range(_MAX_ORFAN_SEARCH_DEPTH):
            count = self.remove_fsrecords_orfans()
            logging.info(f"handle_orfans: Found {i} removed "
                         f"{count} orfans from fsrecords")
            if not count:
                break
        self.handle_file_orfans(clear_orfan_files=clear_orfan_files)
