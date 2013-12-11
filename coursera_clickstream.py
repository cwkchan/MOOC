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
from util.config import *
import sys
from sqlalchemy import *
import ujson #because life is too short
from os.path import basename

def default_json(js):
	"""A default handler for coursera pageview events. This inserts the data into the 
	coursera_clickstream table in batch form."""
	global conn, tbl_coursera_clickstream
	#we first non the value field
	js["value"]=None
	db_batch_insert(conn, tbl_coursera_clickstream, js)

def nested_value_json(js):
	"""A default handler for coursera user.video.lecture.action events, and other events that have
	an extended value field. This inserts the data in the value subparameter into the 
	coursera_clickstream_video table, gets the primary key, then batch inserts data into the 
	clickstream table."""
	global conn, tbl_coursera_clickstream_video, tbl_coursera_clickstream
	try:
		nested_js=ujson.loads(js["value"].encode('ascii','ignore'))
		#this is a workaround since some fields might also be nested
		for key in nested_js.keys():
			if nested_js[key]!=None:
				nested_js[key]=str(nested_js[key])
		rs=conn.execute( tbl_coursera_clickstream_value.insert().values(nested_js))
		#set our foreign key relationship
		js["value"]=rs.inserted_primary_key[0]
		db_batch_insert(conn, tbl_coursera_clickstream, js)
	except Exception, e:
		logger.warn("Exception {} from line {}".format(e,js["value"]))

parser = argparse.ArgumentParser(description='Import coursera clickstream data files into the database')
parser.add_argument('--clean', action='store_true', help='Whether to drop tables in the database or not')
parser.add_argument('--files', action='store', required="True", help='A comma separated list of the files to import')
parser.add_argument('--verbose', action='store_true', help='If this flag exists extended logging will be on')
args = parser.parse_args()

logger=get_logger("coursera_clickstream.py",args.verbose)
conn=get_connection()

if (args.clean):
	query="DROP TABLE IF EXISTS coursera_clickstream"
	try:
		conn.execute(query)
	except:
		pass
	query="DROP TABLE IF EXISTS coursera_clickstream_value"
	try:
		conn.execute(query)
	except:
		pass

query="""CREATE TABLE `coursera_clickstream` (
	`12` varchar(64) DEFAULT NULL,
	`13` varchar(8) DEFAULT NULL,
	`14` text DEFAULT NULL,
	`client` varchar(32) DEFAULT NULL,
  `from` text DEFAULT NULL,
	`id` varchar(255) DEFAULT NULL,
  `key` varchar(64) DEFAULT NULL,
  `language` text DEFAULT NULL,
  `page_url` text DEFAULT NULL,
  `pk` bigint NOT NULL AUTO_INCREMENT,
  `session` varchar(64) DEFAULT NULL,
  `timestamp` bigint(11) NOT NULL,
  `user_agent` text CHARACTER SET utf16 DEFAULT NULL,
  `user_ip` varchar(128) DEFAULT NULL,
  `username` varchar(64) NOT NULL, 
  `value` bigint DEFAULT NULL,
  primary key (pk)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""

conn.execute(query)
	
query="""CREATE TABLE `coursera_clickstream_value` (
  `@` varchar(128) DEFAULT NULL,
  `@candy` text DEFAULT NULL,
  `currentTime` float DEFAULT NULL,
  `error` varchar(8) DEFAULT NULL,
  `eventTimestamp` bigint(20) DEFAULT NULL,
  `fragment` tinytext DEFAULT NULL,
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `initTimestamp` bigint(20) DEFAULT NULL,
  `lectureID` int(11) DEFAULT NULL,
  `networkState` int(11) DEFAULT NULL,
  `paused` varchar(5) DEFAULT NULL,
  `playbackRate` float DEFAULT NULL,
  `prevTime` float DEFAULT NULL,
  `readyState` int(11) DEFAULT NULL,
  `type` varchar(32) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""

conn.execute(query)

metadata = MetaData()
metadata.reflect(bind=conn)
tbl_coursera_clickstream=metadata.tables["coursera_clickstream"]
tbl_coursera_clickstream_value=metadata.tables["coursera_clickstream_value"]

for f in args.files.split(","):
	fid=os.path.splitext(basename(f))[0] #the courseid is this datafilename
	with open(f) as infile:
		logger.info("Working with file {}.".format(f))
		line=" "
		while line!=None and line !="":
			line=infile.readline()
			try:
				js=ujson.loads(line.encode('ascii','ignore'))
				js["id"]=fid
				#check if there is a child value
				if "value" in js.keys():
					nested_value_json(js)
				else:
					default_json(js)
			except Exception, e:
				logger.warn("Exception {} from line {}".format(e,line))
	logger.info("Committing last batch of inserts.")
	db_batch_cleanup(conn, tbl_coursera_clickstream)
	
#at the very end we now have IP addresses in the clickstream table we
#need to run geolocation on
#todo: this should be refactored into a method call, so much bad here
command='python ./util/geolocate.py  --verbose --sql="SELECT DISTINCT user_ip from coursera_clickstream" --schemas="uselab_mooc"'
logger.warn("Calling geolocation using command {}".format(command))
os.system(command)