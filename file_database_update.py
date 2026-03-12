"""File database update module."""
from __future__ import annotations

import logging
from sqlite3 import IntegrityError
from time import CLOCK_MONOTONIC, clock_gettime_ns

from file_database import FileManagerDatabase
from storage_client import StorageClient

_MAX_ORFAN_SEARCH_DEPTH = 256


class FileDatabaseUpdater(FileManagerDatabase):
    def update_file(self, file_full_name: str, storage_client: StorageClient) -> tuple[int, int, int]:
        """Updates or inserts in DB details on file.
            Returns hashed size, file size, hash time in ns."""
        if not self._cur_dir_id:
            raise ValueError("Missing _cur_dir_id")
        (
            fsrecord_id, sha1_read_date, db_file_mtime, file_id, db_file_size, db_file_sha1
        ) = self.get_db_file_info(file_full_name)

        if storage_client.slow_file_read:
            if fsrecord_id and db_file_sha1:
                logging.debug(f"update_file: slow read, skipping hashing for {storage_client.media}/{file_full_name}, " +
                              f"SHA1 in DB {db_file_sha1}, skipped update of fsrecord_id={fsrecord_id}")
                return 0, db_file_size, 0
        else:
            file_name, file_type, size, mtime, _, _ = storage_client.read_file_info(file_full_name, False)
            if size == 0 or file_name == "" and file_type == "":
                logging.warning(f"update_file: size {size} for {storage_client.media}/{file_full_name}, " +
                                (f"skipping update of fsrecord_id={fsrecord_id}" if fsrecord_id else f"skipping insertion into DB"))
                return 0, 0, 0

            if (self._rehash_time < sha1_read_date and
                    db_file_size == size and db_file_mtime == mtime):
                return 0, size, 0   # Current file details matching DB, no re-hash

        file_name, file_type, size, mtime, sha1, hash_time = storage_client.read_file_info(file_full_name, True)
        if not sha1:
            logging.warning(f"Failed to get SHA1 for {file_full_name}, "
                            f"SHA1 in DB {db_file_sha1}, skipped  " +
                            (f"update of fsrecord_id={fsrecord_id}" if fsrecord_id else "insertion into DB"))
            return 0, size, 0   # Skip file if unable to read

        logging.debug(f"Update fsrecord {fsrecord_id} from {storage_client.media}/{file_full_name}, "
                    f"SHA1={sha1}, {size} B, {file_name} {file_type}")
        for attempt in range(2):
            try:
                new_file_id = self.select_update_file_record(
                    sha1, mtime, size, file_name, file_type
                )
                self.update_fsrecord(fsrecord_id, file_full_name, mtime, new_file_id)
                # TODO: Handle deletion of old `files` record file_id
                del file_id
                return size, size, hash_time
            except IntegrityError as error:
                logging.warning(f"update_file: Attempt {attempt + 1} failed to insert file record for {storage_client.media}/{file_full_name}, due to {error}, retrying...")
                continue

        logging.error(f"update_file: Failed to insert file record for {storage_client.media}/{file_full_name} after 3 attempts, ")
        return size, size, hash_time


    def update_files(self, files: list[str], storage_client: StorageClient) -> tuple[int, int, int]:
        """Updating files in current dir.
            Returns hashed size, file size, hash time in ns."""
        hashed_size = 0
        total_size = 0
        total_hash_time = 0
        for file_name in files:
            if storage_client.is_symlink(file_name):
                logging.warning("update_files called for symlink %s/%s",
                                self._cur_dir_path, file_name)
                continue
            hashsed, size, hash_time = self.update_file(file_name, storage_client)
            hashed_size += hashsed
            total_size += size
            total_hash_time += hash_time
        return hashed_size, total_size, total_hash_time

    def print_statistic(
                self, path: str, start_time_ns: int,
                files_count: int, files_hashed_size: int,
                files_total_size: int, files_hash_time_ns: int,
            ) -> None:
        """Calculates and prints dir processing statistic."""
        process_time_ns = clock_gettime_ns(CLOCK_MONOTONIC) - start_time_ns
        average_process_speed = (files_total_size * 1E3) / process_time_ns
        file_process_speed = (files_count * 1E9) / process_time_ns
        if files_hash_time_ns:
            average_hash_time = (files_hashed_size * 1E3) / files_hash_time_ns
        else:
            average_hash_time = 0
        hashing_size_pct = (100 * files_hashed_size / files_total_size
                            if files_total_size else 0)
        hashing_time_pct = 100 * files_hash_time_ns / process_time_ns
        logging.info(f"Processed {path} in {process_time_ns / 1E9:.2f} sec, "
                     f"{files_count} files, {file_process_speed:.2f} files/sec, "
                     f"total size {files_total_size / 1E6:.2f} MB, "
                     f"{average_process_speed:.2f} MB/sec.")
        logging.info(f"In {path} hashed {hashing_size_pct:.1f}% by size, "
                     f"{hashing_time_pct:.1f}% by time, "
                     f"{files_hashed_size / 1E6:.2f} MB in "
                     f"{files_hash_time_ns / 1E9:.2f} sec, "
                     f"{average_hash_time:.2f} MB/sec.")

    def set_cur_dir(self, storage_client: StorageClient) -> None:
        """Saving/updating dir with a path to disk root."""
        if not self._top_dir_id:
            raise ValueError("Missing _top_dir_id")
        self._cur_dir_id = self.get_dir_id(storage_client.get_path_from_mount())
        self._cur_dir_path = storage_client.media
        logging.debug(f"set_cur_dir: {self._cur_dir_path}={self._cur_dir_id}")

    def update_dir(
            self, storage_client: StorageClient,
            max_depth: int | None = 0, check_disk: bool = True
            ) -> tuple[int, int, int, int]:
        """Updating DB with dir details, entrypoint for update_database."""
        dir_path = storage_client.media
        start_time = clock_gettime_ns(CLOCK_MONOTONIC)
        logging.debug(f"update_dir for {dir_path}")
        if check_disk:
            self.set_disk(**storage_client.get_disk_info())
        self.set_cur_dir(storage_client)
        files, sub_dirs = storage_client.read_dir()
        files_count = len(files)
        logging.info("update_dir: %s, %d files, %d sub dirs",
                     dir_path, len(files), len(sub_dirs))
        self.clean_cur_dir(files, is_files=True)
        self.clean_cur_dir(sub_dirs, is_files=False)
        files_hashed_size, files_total_size, files_hash_time_ns = (
            self.update_files(files, storage_client))


        if max_depth != 0:
            for sub_dir_name in sub_dirs:
                storage_client.set_media(dir_path  + '/' + sub_dir_name)
                if storage_client.is_symlink():
                    logging.warning("update_dir called for symlink %s/%s",
                                    dir_path, sub_dir_name)
                    continue
                dir_files_count, dir_hashed_size, dir_size, hash_time_ns = (
                    self.update_dir(
                        storage_client,
                        max_depth=max_depth - 1 if max_depth else None,
                        check_disk=False))
                files_count += dir_files_count
                files_hashed_size += dir_hashed_size
                files_total_size += dir_size
                files_hash_time_ns += hash_time_ns
        self.print_statistic(dir_path, start_time, files_count, files_hashed_size,
                             files_total_size, files_hash_time_ns)
        return (
            files_count, files_hashed_size,
            files_total_size, files_hash_time_ns
        )

    def handle_orfans(self, clear_orfan_files: bool = False):
        """Removes fsrecords and file orfans."""
        for i in range(_MAX_ORFAN_SEARCH_DEPTH):
            count = self.remove_fsrecords_orfans()
            logging.info(f"handle_orfans: Round {i} removed "
                         f"{count} orfans from fsrecords")
            if not count:
                break
        self.handle_file_orfans(clear_orfan_files=clear_orfan_files)
