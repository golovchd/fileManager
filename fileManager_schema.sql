BEGIN TRANSACTION;
CREATE TABLE IF NOT EXISTS "types" (
	"name"	TEXT UNIQUE,
	"extensions"	TEXT UNIQUE
);
CREATE TABLE IF NOT EXISTS "files" (
	"FileSize"	INTEGER NOT NULL,
	"SHA1"	TEXT NOT NULL,
	"EarliestDate"	INTEGER,
	"CanonicalName"	TEXT,
	"CanonicalType"	TEXT,
	"MediaType"	INTEGER,
	FOREIGN KEY("MediaType") REFERENCES "types"("ROWID") ON DELETE SET NULL
);
CREATE TABLE IF NOT EXISTS "disks" (
	"UUID"	TEXT NOT NULL UNIQUE,
	"DiskSize"	INTEGER NOT NULL,
	"Label"	TEXT
);
CREATE TABLE IF NOT EXISTS "fsrecords" (
	"Name"	TEXT NOT NULL,
	"ParentId"	INTEGER,
	"DiskId"	INTEGER NOT NULL,
	"FileDate"	INTEGER,
	"FileId"	INTEGER,
	"ParentPath"	TEXT,
	FOREIGN KEY("FileId") REFERENCES "files"("ROWID") ON DELETE CASCADE,
	FOREIGN KEY("ParentId") REFERENCES "fsrecords"("ROWID") ON DELETE CASCADE,
	FOREIGN KEY("DiskId") REFERENCES "disks"("ROWID") ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS "file_size" ON "files" (
	"FileSize"	ASC
);
CREATE UNIQUE INDEX IF NOT EXISTS "file_sha1" ON "files" (
	"SHA1"	ASC
);
CREATE INDEX IF NOT EXISTS "file_date" ON "files" (
	"EarliestDate"	ASC
);
CREATE UNIQUE INDEX IF NOT EXISTS "disk_uuid" ON "disks" (
	"UUID"	ASC
);
CREATE INDEX IF NOT EXISTS "disk_size" ON "disks" (
	"DiskSize"	ASC
);
CREATE INDEX IF NOT EXISTS "parent_id" ON "fsrecords" (
	"ParentId"	ASC
);
CREATE TRIGGER `fsrecords_on_delete` AFTER DELETE ON `fsrecords` BEGIN
	DELETE FROM `fsrecords` WHERE `ParentId` = old.`ROWID`;
END;
COMMIT;
