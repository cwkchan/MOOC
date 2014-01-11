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
from util.url_parser import *
import sys
from sqlalchemy import *
import ujson #because life is too short
from os.path import basename
from os import listdir


def __json_parser(js):
    """This inserts the data into the coursera_clickstream table in batch form."""
    global conn, tbl_coursera_clickstream
    js = __value_parser(js)
    js = __url_parser(js)
    db_batch_insert(conn, tbl_coursera_clickstream, js)


def __url_parser(js):
    """Parses out a coursera resource access and puts it in table
    coursera_clickstream_url.  Foreign key is returned as js['url']"""
    global conn, tbl_coursera_clickstream_url
    if "page_url" not in js.keys() or js["page_url"] == "" or js["page_url"] is None:
        js["url"] = None
        return js
    try:
        url_info = {}
        url_info["course_id"], url_info["path"], url_info["resource"], url_info["parameters"] = url_to_tuple(
            js["page_url"])
        rs = conn.execute(tbl_coursera_clickstream_url.insert().values(url_info))
        #set our foreign key relationship
        js["url"] = rs.inserted_primary_key[0]
    except InvalidCourseraUrlException, e1:
        logger.warn("Url {} is not detected as a Coursera clickstream URL".format(e1.value))
        js["url"] = None
    except Exception, e:
        logger.warn("Exception {} from line {}".format(e, js["page_url"]))
    return js


def __value_parser(js):
    """Parses the value field which should be json. This inserts data into
    the coursera_clickstream_value table and returns the key as js['value']."""
    global conn, tbl_coursera_clickstream_value
    if "value" not in js.keys() or js["value"] == "" or js["value"] == None or js["value"] == "{}":
        js["value"] = None
        return js
    try:
        nested_js = ujson.loads(js["value"].encode('ascii', 'ignore'))
        #this is a workaround since some fields might also be nested
        for key in nested_js.keys():
            if nested_js[key] != None:
                nested_js[key] = str(nested_js[key])
        rs = conn.execute(tbl_coursera_clickstream_value.insert().values(nested_js))
        #set our foreign key relationship
        js["value"] = rs.inserted_primary_key[0]
        return js
    except Exception, e:
        logger.warn("Exception {} from line {}".format(e, js["value"]))


parser = argparse.ArgumentParser(description='Import coursera clickstream data files into the database')
parser.add_argument('--clean', action='store_true', help='Whether to drop tables in the database or not')
parser.add_argument('--verbose', action='store_true', help='If this flag exists extended logging will be on')
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--files', help='A comma separated list of the files to import')
group.add_argument('--dir', help='A directory with Coursera clickstream logs')
args = parser.parse_args()

logger = get_logger("coursera_clickstream.py", args.verbose)

files = []
if args.dir is not None:
    files = listdir(args.dir)
    files = map(lambda x: args.dir + x, files)
else:
    if args.files is not None:
        files = args.files.split(',')
    else:
        raise Exception("One of files or directory is require.")

conn = get_connection()

if (args.clean):
    query = "DROP TABLE IF EXISTS coursera_clickstream"
    try:
        conn.execute(query)
    except:
        pass
    query = "DROP TABLE IF EXISTS coursera_clickstream_value"
    try:
        conn.execute(query)
    except:
        pass
    query = "DROP TABLE IF EXISTS coursera_clickstream_url"
    try:
        conn.execute(query)
    except:
        pass

    query = """CREATE TABLE `coursera_clickstream` (
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
      `url` bigint DEFAULT NULL,
      `user_agent` text CHARACTER SET utf16 DEFAULT NULL,
      `user_ip` varchar(128) DEFAULT NULL,
      `username` varchar(64) NOT NULL,
      `value` bigint DEFAULT NULL,
      primary key (pk)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
    """

    conn.execute(query)

    query = """CREATE TABLE `coursera_clickstream_value` (
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

    query = """CREATE TABLE `coursera_clickstream_url` (
      `course_id` varchar(128) NOT NULL,
      `id` bigint(20) NOT NULL AUTO_INCREMENT,
        `parameters` text DEFAULT NULL,
        `path` varchar(128) DEFAULT NULL,
        `resource` varchar(128) DEFAULT NULL,
      PRIMARY KEY (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
    """

    conn.execute(query)

metadata = MetaData()
metadata.reflect(bind=conn)
tbl_coursera_clickstream = metadata.tables["coursera_clickstream"]
tbl_coursera_clickstream_value = metadata.tables["coursera_clickstream_value"]
tbl_coursera_clickstream_url = metadata.tables["coursera_clickstream_url"]

for f in files:
    fid = os.path.splitext(basename(f))[0] #the courseid is this datafilename
    with open(f) as infile:
        logger.info("Working with file {}.".format(f))
        line = " "
        while line != None and line != "":
            line = infile.readline()
            try:
                js = ujson.loads(line.encode('ascii', 'ignore'))
                js["id"] = fid
                __json_parser(js)
            except Exception, e:
                logger.warn("Exception {} from line {}".format(e, line))
    logger.info("Committing last batch of inserts.")
    db_batch_cleanup(conn, tbl_coursera_clickstream)

#at the very end we now have IP addresses in the clickstream table we
#need to run geolocation on
#todo: this should be refactored into a method call, so much bad here
command = 'python ./util/geolocate.py  --verbose --sql="SELECT DISTINCT user_ip from coursera_clickstream" --schemas="uselab_mooc"'
logger.warn("Calling geolocation using command {}".format(command))
os.system(command)