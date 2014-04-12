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

import Queue
import argparse
from threading import Condition
from util.config import *
from util.url_parser import *
import sys
from sqlalchemy import *
import json #because life is too short
from os.path import basename
from os import listdir


def __json_parser(js):
    """This inserts the data into the coursera_clickstream table in batch form."""
    global queue
    js = __value_parser(js)
    js = __url_parser(js)
    js = __flatten_lists(js)
    queue.put(js)


def __flatten_lists(js, keys=['12', '13', '14']):
    """for each js[key] in keys it calls str()"""
    for key in keys:
        if key in js:
            js[key] = str(js[key])
    return js


def __url_parser(js):
    """Parses out a coursera resource access as js["page_url"] and puts it in columns prepended with 'url'"""
    if not ( "page_url" not in js.keys() or js["page_url"] == "" or js["page_url"] is None):
        try:
            url_info = {}
            course, url_info["url_path"], url_info["url_resource"], url_info["url_parameters"] = url_to_tuple(js["page_url"])
        except InvalidCourseraUrlException, e1:
            logger.warn("Url {} is not detected as a Coursera clickstream URL".format(e1.value))
        except Exception, e:
            logger.warn("Exception {} from line {}".format(e, js["page_url"]))
    return js


def __value_parser(js):
    """Parses the js["value"] field which should be json.  Results are put into js as top level keys prepended with
    the string 'val' """
    if not ("value" not in js.keys() or js["value"] == "" or js["value"] == None or js["value"] == "{}"):
        try:
            nested_js = json.loads(js["value"].encode('ascii', 'ignore'))
            #this is a workaround since some fields might also be nested
            for key in nested_js.keys():
                if nested_js[key] != None:
                    nested_js[key] = str(nested_js[key])
                js["value_{}".format(key)] = nested_js[key]
        except Exception, e:
            logger.warn("Exception {} from line {}".format(e, js["value"]))
    if "value" in js:
        del js["value"]
    return js

def __queue_control(queue, maxsize=100000):
    """Checks to see if the queue is full and pauses for catchup if neccessary """
    while queue.qsize() >= maxsize:
        cv = Condition(threading.RLock())
        cv.acquire()
        cv.wait(1)
        cv.release()

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

if args.clean:
    query = "DROP TABLE IF EXISTS coursera_clickstream"
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
      `id` int DEFAULT NULL,
      `key` text DEFAULT NULL,
      `language` text DEFAULT NULL,
      `page_url` text DEFAULT NULL,
      `pk` bigint NOT NULL AUTO_INCREMENT,
      `session` varchar(64) DEFAULT NULL,
      `timestamp` bigint(11) DEFAULT NULL,
      `user_agent` text CHARACTER SET utf16 DEFAULT NULL,
      `user_ip` varchar(128) DEFAULT NULL,
      `username` varchar(64) DEFAULT NULL,

      `value_@` varchar(128) DEFAULT NULL,
      `value_@candy` text DEFAULT NULL,
      `value_currentTime` float DEFAULT NULL,
      `value_error` varchar(8) DEFAULT NULL,
      `value_eventTimestamp` bigint(20) DEFAULT NULL,
      `value_fragment` tinytext DEFAULT NULL,
      `value_initTimestamp` bigint(20) DEFAULT NULL,
      `value_lectureID` int(11) DEFAULT NULL,
      `value_networkState` int(11) DEFAULT NULL,
      `value_paused` varchar(5) DEFAULT NULL,
      `value_playbackRate` float DEFAULT NULL,
      `value_prevTime` float DEFAULT NULL,
      `value_readyState` int(11) DEFAULT NULL,
      `value_type` varchar(32) DEFAULT NULL,

      `url_parameters` text DEFAULT NULL,
      `url_path` varchar(128) DEFAULT NULL,
      `url_resource` varchar(128) DEFAULT NULL,

      PRIMARY KEY (pk)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
    """

    conn.execute(query)

#Discover the table to insert data into
metadata = MetaData()
metadata.reflect(bind=conn)
tbl_coursera_clickstream = metadata.tables["coursera_clickstream"]

for f in files:
    fid = convert_sessionid_to_id(os.path.splitext(basename(f))[0]) #the courseid is this datafilename, convert to pk
    with open(f) as infile:
        #Start database queue
        queue = Queue.Queue()
        dbq = ThreadedDBQueue(queue, conn, tbl_coursera_clickstream, batch_size=10000, log_to_console=args.verbose, hard_exit_on_failure=True)
        dbq.start()

        logger.info("Working with file {}.".format(f))
        line = " "
        while line is not None and line != "":
            line = infile.readline()
            try:
                js = json.loads(line.encode('ascii', 'ignore'))
                js["id"] = fid
                __json_parser(js)
                __queue_control(queue)
            except Exception, e:
                logger.warn("Exception {} from line '{}'".format(e, line))
        #Wait for the db injector to finish processing this file
        dbq.stop()
        logger.info("Waiting for database thread to complete data insertions.")
        dbq.join()

#at the very end we now have IP addresses in the clickstream table we
#need to run geolocation on
#todo: this should be refactored into a method call, so much bad here
#command = 'python ./util/geolocate.py  --verbose --sql="SELECT DISTINCT user_ip from coursera_clickstream" --schemas="uselab_mooc"'
#logger.warn("Calling geolocation using command {}".format(command))
#os.system(command)