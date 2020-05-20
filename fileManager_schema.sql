BEGIN TRANSACTION;
CREATE TABLE IF NOT EXISTS `types` (
	`name`				TEXT UNIQUE,
	`extensions`	TEXT UNIQUE
);
CREATE TABLE IF NOT EXISTS `fsrecords` (
	`Name`			TEXT NOT NULL,
	`ParentId`	INTEGER,
	`DiskId`		INTEGER NOT NULL,
	`FileDate`	INTEGER,
	`FileId`	 	INTEGER,
	FOREIGN KEY(`ParentId`) REFERENCES `fsrecords`(`ROWID`)
	FOREIGN KEY(`DiskId`) REFERENCES `disks`(`ROWID`)
	FOREIGN KEY(`FileId`) REFERENCES `files`(`ROWID`)
);
CREATE TABLE IF NOT EXISTS `files` (
	`FileSize`			INTEGER NOT NULL,
	`MD5`						TEXT NOT NULL,
	`EarliestDate`	INTEGER,
	`CanonicalName`	TEXT,
	`CanonicalType`	TEXT,
	`MediaType`			INTEGER,
	FOREIGN KEY(`MediaType`) REFERENCES `types`(`ROWID`)
);
CREATE TABLE IF NOT EXISTS `disks` (
	`UUID`			TEXT NOT NULL UNIQUE,
	`DiskSize`	INTEGER NOT NULL,
	`Label`			TEXT
);
CREATE INDEX IF NOT EXISTS `file_size` ON `files` (
	`FileSize`	ASC
);
CREATE UNIQUE INDEX IF NOT EXISTS `file_md5` ON `files` (
	`MD5`	ASC
);
CREATE INDEX IF NOT EXISTS `file_date` ON `files` (
	`EarliestDate`	ASC
);
CREATE UNIQUE INDEX IF NOT EXISTS `disk_uuid` ON `disks` (
	`UUID`	ASC
);
CREATE INDEX IF NOT EXISTS `disk_size` ON `disks` (
	`DiskSize`	ASC
);
CREATE INDEX IF NOT EXISTS `parent_id` ON `fsrecords` (
	`ParentId`	ASC
);
COMMIT;
