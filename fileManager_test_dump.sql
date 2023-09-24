BEGIN TRANSACTION;
CREATE TABLE "disks" (
	"UUID"	TEXT NOT NULL UNIQUE,
	"DiskSize"	INTEGER NOT NULL,
	"Label"	TEXT
);
INSERT INTO "disks" VALUES('0a2e2cb7-4543-43b3-a04a-40959889bd45',59609420,'');
CREATE TABLE "files" (
	"FileSize"	INTEGER NOT NULL,
	"SHA1"	TEXT NOT NULL,
	"EarliestDate"	INTEGER,
	"CanonicalName"	TEXT,
	"CanonicalType"	TEXT,
	"MediaType"	INTEGER,
	FOREIGN KEY("MediaType") REFERENCES "types"("ROWID") ON DELETE SET NULL
);
INSERT INTO "files" VALUES(239027,'db85197ec899df5cbd2a7fb28bf30a1b9875f2ed',1.69543415957339358322e+09,'6TB-2 benchmark 2018-08-25 20-58-29','png',NULL);
INSERT INTO "files" VALUES(3473408,'f8b1465e6340d11f2a28d26cf896e6427ab41f63',1.6954341596453895569e+09,'DSC06979','JPG',NULL);
INSERT INTO "files" VALUES(2479559,'7d81cd88335556e91c6cb35bb683cbb0a9b411fb',1.69543415965338921543e+09,'IMG_0004','JPG',NULL);
INSERT INTO "files" VALUES(3242996,'f7131eaafe02290403d0c96837e69e99134749a2',1.69543415966538858416e+09,'IMG_0013','JPG',NULL);
INSERT INTO "files" VALUES(16,'c19dfe09be521ccdf6957794128aef97c592baf6',1.69543415966538858416e+09,'not.an.image','txt',NULL);
INSERT INTO "files" VALUES(18,'956f4ca7fd5877604544213c6b66b33416ebfb3f',1.69543415966538858416e+09,'not_an_image','',NULL);
INSERT INTO "files" VALUES(1215763,'c81bb242e63bb1296fa1062fe5b3118f476193c9',1.6954341597253854275e+09,'DSC06979c','JPG',NULL);
INSERT INTO "files" VALUES(0,'da39a3ee5e6b4b0d3255bfef95601890afd80709',1.69544707447482919694e+09,'foo','txt',NULL);
INSERT INTO "files" VALUES(2902816,'db5da8c807516b52f961ea7df4abe8160943f619',1.69543415974938416483e+09,'DSC06979','JPG',NULL);
INSERT INTO "files" VALUES(2348583,'86463569661fe9bad6cfbd15e8cb8b5552d5fc90',1.69543415976938295361e+09,'IMG_0004','JPG',NULL);
CREATE TABLE "fsrecords" (
	"Name"	TEXT NOT NULL,
	"ParentId"	INTEGER,
	"DiskId"	INTEGER NOT NULL,
	"FileDate"	INTEGER,
	"FileId"	INTEGER,
	"ParentPath"	TEXT,
	"SHA1ReadDate"	INTEGER,
	FOREIGN KEY("FileId") REFERENCES "files"("ROWID") ON DELETE CASCADE,
	FOREIGN KEY("DiskId") REFERENCES "disks"("ROWID") ON DELETE CASCADE,
	FOREIGN KEY("ParentId") REFERENCES "fsrecords"("ROWID") ON DELETE CASCADE
);
INSERT INTO "fsrecords" VALUES('',NULL,1,NULL,NULL,NULL,NULL);
INSERT INTO "fsrecords" VALUES('home',1,1,NULL,NULL,NULL,NULL);
INSERT INTO "fsrecords" VALUES('dimagolov',2,1,NULL,NULL,NULL,NULL);
INSERT INTO "fsrecords" VALUES('git',3,1,NULL,NULL,NULL,NULL);
INSERT INTO "fsrecords" VALUES('fileManager',4,1,NULL,NULL,NULL,NULL);
INSERT INTO "fsrecords" VALUES('test_data',5,1,NULL,NULL,NULL,NULL);
INSERT INTO "fsrecords" VALUES('media',6,1,NULL,NULL,NULL,NULL);
INSERT INTO "fsrecords" VALUES('6TB-2 benchmark 2018-08-25 20-58-29.png',7,1,1.69543415957339358322e+09,1,NULL,1.69559020969606828689e+09);
INSERT INTO "fsrecords" VALUES('DSC06979.JPG',7,1,1.6954341596453895569e+09,2,NULL,1.69559020973498487472e+09);
INSERT INTO "fsrecords" VALUES('IMG_0004.JPG',7,1,1.69543415965338921543e+09,3,NULL,1.69559020977053117752e+09);
INSERT INTO "fsrecords" VALUES('IMG_0013.JPG',7,1,1.69543415966538858416e+09,4,NULL,1.69559020981002426141e+09);
INSERT INTO "fsrecords" VALUES('not.an.image.txt',7,1,1.69543415966538858416e+09,5,NULL,1.69559020983142638206e+09);
INSERT INTO "fsrecords" VALUES('not_an_image',7,1,1.69543415966538858416e+09,6,NULL,1.69559020985379862783e+09);
INSERT INTO "fsrecords" VALUES('storage',6,1,NULL,NULL,NULL,NULL);
INSERT INTO "fsrecords" VALUES('DSC06979 (copy).JPG',14,1,1.69543415968938732153e+09,2,NULL,1.69559020989512157441e+09);
INSERT INTO "fsrecords" VALUES('DSC06979.JPG',14,1,1.69543415971338605873e+09,2,NULL,1.6955902099094469547e+09);
INSERT INTO "fsrecords" VALUES('DSC06979c.JPG',14,1,1.6954341597253854275e+09,7,NULL,1.69559020993184852597e+09);
INSERT INTO "fsrecords" VALUES('second_dir',14,1,NULL,NULL,NULL,NULL);
INSERT INTO "fsrecords" VALUES('foo.txt',18,1,1.69544707447482919694e+09,8,NULL,1.69559020996877121926e+09);
INSERT INTO "fsrecords" VALUES('tagged',14,1,NULL,NULL,NULL,NULL);
INSERT INTO "fsrecords" VALUES('DSC06979.JPG',20,1,1.69543415974938416483e+09,9,NULL,1.69559021002043628688e+09);
INSERT INTO "fsrecords" VALUES('IMG_0004.JPG',20,1,1.69543415976938295361e+09,10,NULL,1.69559021004032182697e+09);
CREATE TABLE "types" (
	"name"	TEXT UNIQUE,
	"extensions"	TEXT UNIQUE
);
CREATE INDEX "file_size" ON "files" (
	"FileSize"	ASC
);
CREATE UNIQUE INDEX "file_sha1" ON "files" (
	"SHA1"	ASC
);
CREATE INDEX "file_date" ON "files" (
	"EarliestDate"	ASC
);
CREATE UNIQUE INDEX "disk_uuid" ON "disks" (
	"UUID"	ASC
);
CREATE INDEX "disk_size" ON "disks" (
	"DiskSize"	ASC
);
CREATE INDEX "parent_id" ON "fsrecords" (
	"ParentId"	ASC
);
CREATE TRIGGER `fsrecords_on_delete` AFTER DELETE ON `fsrecords` BEGIN
	DELETE FROM `fsrecords` WHERE `ParentId` = old.`ROWID`;
END;
COMMIT;
