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

def __default_pageview_handler(js):
	"""A default handler for coursera pageview events. This inserts the data into the 
	coursera_clickstream table in batch form."""
	global conn, tbl_coursera_clickstream
	#we first non the value field
	js["value"]=None
	db_batch_insert(conn, tbl_coursera_clickstream, js)

def __default_uservideolectureaction_handler(js):
	"""A default handler for coursera user.video.lecture.action events. This inserts the data
	in the value subparameter into the coursera_clickstream_video table, gets the primary key,
	then batch inserts data into the clickstream table."""
	global conn, tbl_coursera_clickstream_video, tbl_coursera_clickstream
	try:
		nested_js=ujson.loads(js["value"].encode('ascii','ignore'))
		rs=conn.execute( tbl_coursera_clickstream_video.insert().values(nested_js))
		#set our foreign key relationship
		js["value"]=rs.inserted_primary_key[0]
		db_batch_insert(conn, tbl_coursera_clickstream, js)
	except Exception, e:
		logger.warn("Exception {} from line {}".format(e,js["value"]))

__coursera_clickstream_handlers={"pageview":__default_pageview_handler, 
		"user.video.lecture.action": __default_uservideolectureaction_handler
		}
"""A list of handler functions for coursera clickstream events """

parser = argparse.ArgumentParser(description='Import coursera clickstream data files into the database')
parser.add_argument('--clean', action='store_true', help='Whether to drop tables in the database or not')
parser.add_argument('--files', action='store', help='A comma separated list of the files to import')
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

if (args.clean):
	query="DROP TABLE IF EXISTS coursera_clickstream_video"
	try:
		conn.execute(query)
	except:
		pass

query="""CREATE TABLE `coursera_clickstream` (
	`id` varchar(255) DEFAULT NULL,
  `key` varchar(64) DEFAULT NULL,
  `page_url` varchar(2048) DEFAULT NULL,
  `user_agent` varchar(1024) DEFAULT NULL,
  `session` varchar(64) DEFAULT NULL,
  `user_ip` varchar(128) DEFAULT NULL,
  `client` varchar(32) DEFAULT NULL,
  `value` bigint DEFAULT NULL,
  `timestamp` bigint(11) NOT NULL,
  `language` varchar(512) DEFAULT NULL,
  `from` varchar(1024) DEFAULT NULL,
  `username` varchar(64) NOT NULL, 
  `pk` bigint NOT NULL AUTO_INCREMENT,
  primary key (pk)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""

conn.execute(query)

query="""CREATE TABLE `coursera_clickstream_video` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `readyState` int(11) DEFAULT NULL,
  `initTimestamp` bigint(20) DEFAULT NULL,
  `type` varchar(32) DEFAULT NULL,
  `playbackRate` float DEFAULT NULL,
  `paused` varchar(5) DEFAULT NULL,
  `lectureID` int(11) DEFAULT NULL,
  `networkState` int(11) DEFAULT NULL,
  `eventTimestamp` bigint(20) DEFAULT NULL,
  `error` varchar(128) DEFAULT NULL,
  `prevTime` float DEFAULT NULL,
  `currentTime` float DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""

conn.execute(query)

metadata = MetaData()
metadata.reflect(bind=conn)
tbl_coursera_clickstream=metadata.tables["coursera_clickstream"]
tbl_coursera_clickstream_video=metadata.tables["coursera_clickstream_video"]

for f in args.files.split(","):
	with open(f) as infile:
		line=" "
		while line!=None and line !="":
			line=infile.readline()
			try:
				js=ujson.loads(line.encode('ascii','ignore'))
				js["id"]=f #the courseid is this datafilename
				fun=__coursera_clickstream_handlers.get(str(js["key"]))
				if fun != None:
					fun(js)
			except Exception, e:
				logger.warn("Exception {} from line {}".format(e,line))
	db_batch_cleanup(conn, tbl_coursera_clickstream)