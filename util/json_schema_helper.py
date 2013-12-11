#    Copyright (C) 2013 The Regents of the University of Michigan
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see [http://www.gnu.org/licenses/].

import argparse
import sys
import ujson
from config import *
from os import listdir
from collections import defaultdict
import signal
import pprint

"""The purpose of this script is to try and help identify appropriate RDBMS schema
elements for JSON data.  E.g., the Coursera clickstream data is all json, sometimes
with unpredictable data formats.

This script may be useful in building other scripts, but not much use when running
actual analysis.
"""
parser = argparse.ArgumentParser(description='Parse JSON and identify schemas')
parser.add_argument('--verbose', action='store_true', help='If this flag exists extended logging will be on')
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--files', help='A list of Coursera JSON clickstream files')
group.add_argument('--dir', help='A directory with CSV files in it')
args = parser.parse_args()

logger=get_logger("json_schema_helper.py",args.verbose)

if (args.dir!=None):
	files = listdir(args.dir)
	files = map(lambda x:args.dir+x,files)
else:
	files = args.files.split(',')

#store our results in here
attributes_length={}
attributes_values={}
course_wide_attributes_length={}
course_wide_attributes_values={}

def __is_json(line):
	"""Returns bool, json"""
	try:
		js=ujson.loads(line.encode('ascii','ignore'))
		#special case, if it is an int, or string, or unicode it is also json, but we want to terminate
		if type(js)==int:
			return False, None
		if type(js)==str:
			return False, None
		if type(js)==unicode:
			return False, None
		if type(js)==float:
			return False, None
		return True, js
	except:
		return False, None

def __examine_json(js, key=None, prefix=""):
	"""Examines a snipit of json recursively, returning a dictionary of 
	elements and the length each one is when converted to a str, or a dict"""
	return_vals={}
	for item in js.keys():
		#recurse
		isjs,newjs=__is_json(js[item])
		if isjs:
			__examine_json(newjs, key=key, prefix=item+"%")
		#should probably figure out defaultdict instead
		if prefix+item not in attributes_length[key].keys():
			attributes_length[key][prefix+item]=0
			attributes_values[key][prefix+item]=""
		#really inefficient to do it this way, but its quick to code
		if prefix+item not in course_wide_attributes_length.keys():
			course_wide_attributes_length[prefix+item]=0
			course_wide_attributes_values[prefix+item]=""
		#check if it is bigger for this course
		if len(str(js[item])) > attributes_length[key][prefix+item]:
			attributes_length[key][prefix+item]=len(str(js[item]))
			attributes_values[key][prefix+item]=str(js[item])
		#check across all courses
		if len(str(js[item])) > course_wide_attributes_length[prefix+item]:
			course_wide_attributes_length[prefix+item]=len(str(js[item]))
			course_wide_attributes_values[prefix+item]=str(js[item])

def __signal_handler(signal, frame):
	"""Print the results gathered thus far and quit"""
	__print_results()
	sys.exit(0)

def __print_results():
	for item in attributes_length.keys():
		print "Key value {} has attributes_length:".format(item)
		pprint.pprint(attributes_length[item])
		print "\n"
		print "And key value {} has attributes_values:".format(item)
		pprint.pprint(attributes_values[item])		
		print "\n********************************\n"
	print "Results across all data input files: "
	print "course_wide_attributes_length"
	pprint.pprint(course_wide_attributes_length)
	print "\n"
	print "course_wide_attributes_values"
	pprint.pprint(course_wide_attributes_values)
	print "\n********************************\n"
		
signal.signal(signal.SIGINT, __signal_handler)		

for f in files:
	logger.debug("Examining file {}.".format(f))
	with open(f) as infile:
		line=" "
		while line!=None and line !="":
			line=infile.readline()
			isjs,js=__is_json(line)
			if isjs:
				#check if this key already exists in the attribute dict
				if js["key"] not in attributes_length:
					attributes_length[js["key"]]={}
					attributes_values[js["key"]]={}
				__examine_json(js, key=js["key"])
__print_results()