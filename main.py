#!/usr/bin/python
import argparse
import hashlib
import os
import stat
import sys

class fileDetails:
	def __init__(self, path, name, useContent= False):
		self.path= path
		self.name= name
		self.stat= os.stat(os.path.join(path, name))
		if useContent:
			self.md5= hashlib.md5(open(os.path.join(path, name), 'rb').read())
		else:
			self.md5= hashlib.md5(name + str(self.stat))
#		if printVerbose:
#			sys.stdout.write("File: {0}\{1} size:{2}\n".format(path, name, self.stat.st_size));

#class folderDetails:
#	def __init__(self):


class folderProcessor:
	def __init__(self, useHash, printVerbose):
		self.useHash= useHash
		self.printVerbose= printVerbose

	def process(self, Location, checkType= True):
		file_list= []
		name_ref= {}
		name_size_ref= {}
		content_ref= {}
		if checkType:
			locationStat= os.stat(Location)
			if self.printVerbose:
				sys.stdout.write("Location {0} is dir: {1}\n".format(args.Location, stat.S_ISDIR(locationStat.st_mode)));
		else:
			sys.stdout.write("Skip type check for Location {0}\n".format(args.Location));
		if not checkType or stat.S_ISDIR(locationStat.st_mode):
			for dirpath, dirnames, filenames in os.walk(Location):
				for file in filenames:
					file_list.append(fileDetails(dirpath, file, self.useHash))
					idx= len(file_list) - 1
					if file in name_ref:
						name_ref[file].append(idx)
						if self.printVerbose:
							sys.stdout.write("Entry info:\t{0}\{1} size:{2}\n".format(dirpath, file, file_list[idx].stat.st_size));
							sys.stdout.write("\tduplicates by name with: {0} files\n".format(len(name_ref[file])));
					else:
						name_ref[file]= [idx]
					name_size= "{0}_{1}".format(file, file_list[idx].stat.st_size)
					if name_size in name_size_ref:
						name_size_ref[name_size].append(idx)
						if self.printVerbose:
							sys.stdout.write("Entry info:\t{0}\{1} size:{2}\n".format(dirpath, file, file_list[idx].stat.st_size));
							sys.stdout.write("\tduplicates by name/size with: {0} files\n".format(len(name_size_ref[name_size])));
					else:
						name_size_ref[name_size]= [idx]
					if self.useHash:
						md5= file_list[idx].md5
						if md5 in content_ref:
							content_ref[md5].append(idx)
							if self.printVerbose:
								sys.stdout.write("Entry info:\t{0}\{1} size:{2}\n".format(dirpath, file, file_list[idx].stat.st_size));
								sys.stdout.write("\tduplicates by content with: {0} files\n".format(len(content_ref[md5])));
						else:
							content_ref[md5]= [idx]
			
	
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
except IOError as e:
	sys.stderr.write("Error while processing location {0}:\n\t{1}\n".format(args.Location, str(e)));
else:
	sys.stdout.write("Finished processing location {0}\n".format(args.Location));
