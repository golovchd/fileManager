# fileManager

File manager tools to maintain files archive.

Tool is using local sqlite3 DB (default database location is `/var/lib/file-manager/fileManager.db`),
please specify database using param `--database` if you want to use other location.

## Dependencies

Please install `uv` (e.g. `pip install uv`), it will handle python dependencies

## Commands

Run commands below using `uv` (`uv run <command nmame>`), use `-h`/`--help` to get details on parameters

- `update_database` - Import data from specific storage (disk location or S3 path) into database
- `file-manager` - Analysis of database content
- `check-duplicates` - Search and removal of duplicated files on specific disk
- `import-media` - Import media files to storage location. Prototype, work in progress
