# Copyright (C) 2013 The Regents of the University of Michigan
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
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

import multiprocessing
import argparse
from threading import Condition, RLock
from util.config import *
from util.url_parser import *
import sys
from sqlalchemy import *
import ujson  # because life is too short
from os.path import basename
from os import listdir
import gzip
import time


def __json_parser(js, queue):
    """This inserts the data into the coursera_clickstream table in batch form."""
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
            course, url_info["url_path"], url_info["url_resource"], url_info["url_parameters"] = url_to_tuple(
                js["page_url"])
            for key in url_info.keys():
                js[key] = url_info[key]
        except InvalidCourseraUrlException as e1:
            logger.warn("Url {} is not detected as a Coursera clickstream URL".format(e1.value))
        except Exception as e:
            logger.warn("Exception {} from line {}".format(e, js["page_url"]))
    return js


def __value_parser(js):
    """Parses the js["value"] field which should be json.  Results are put into js as top level keys prepended with
    the string 'val' """
    if not ("value" not in js.keys() or js["value"] == "" or js["value"] == None or js["value"] == "{}"):
        try:
            nested_js = ujson.loads(js["value"])
            #this is a workaround since some fields might also be nested
            for key in nested_js.keys():
                if nested_js[key] != None:
                    nested_js[key] = str(nested_js[key])
                js["value_{}".format(key)] = nested_js[key]
        except Exception as e:
            logger.warn("Exception {} from line {}".format(e, js["value"]))
    if "value" in js:
        del js["value"]
    return js


def __queue_control(queue, maxsize=100000):
    """Checks to see if the queue is full and pauses for catchup if neccessary """
    while queue.qsize() >= maxsize:
        cv = Condition(RLock())
        cv.acquire()
        cv.wait(10)
        cv.release()


parser = argparse.ArgumentParser(description='Import coursera clickstream data files into the database')
parser.add_argument('--clean', action='store_true', help='Whether to drop tables in the database or not')
parser.add_argument('--verbose', action='store_true', help='If this flag exists extended logging will be on')
parser.add_argument('--output',  help='The output file.  If this is not specified data will be stored directly in the'
                                      'database (slow).  If this is specified data will be stored in a CSV file at the'
                                      'given location.  Data can be loaded from the CSV file with: LOAD DATA LOCAL '
                                      'INFILE "/tmp/filename.csv" INTO TABLE uselab_mooc.coursera_clickstream IGNORE 1 '
                                      'LINES; (fast).  Note: the output file must be easily accessible by MySQL (e.g.'
                                      'stored in /tmp)')
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
BATCH_SIZE = 1000

if args.clean:
    query = "DROP TABLE IF EXISTS coursera_clickstream"
    try:
        conn.execute(query)
    except:
        pass

    query = """CREATE TABLE `coursera_clickstream` (
      `12` VARCHAR(64) DEFAULT NULL,
      `13` VARCHAR(8) DEFAULT NULL,
      `14` TEXT DEFAULT NULL,
      `30` TEXT DEFAULT NULL,
      `client` VARCHAR(32) DEFAULT NULL,
      `from` TEXT DEFAULT NULL,
      `id` INT DEFAULT NULL,
      `key` TEXT DEFAULT NULL,
      `language` TEXT DEFAULT NULL,
      `page_url` TEXT DEFAULT NULL,
      `session` VARCHAR(64) DEFAULT NULL,
      `timestamp` BIGINT(11) DEFAULT NULL,
      `user_agent` TEXT CHARACTER SET utf16 DEFAULT NULL,
      `user_ip` VARCHAR(128) DEFAULT NULL,
      `username` VARCHAR(64) DEFAULT NULL,

      `value_@` VARCHAR(128) DEFAULT NULL,
      `value_@candy` TEXT DEFAULT NULL,
      `value_currentTime` FLOAT DEFAULT NULL,
      `value_error` VARCHAR(8) DEFAULT NULL,
      `value_eventTimestamp` BIGINT(20) DEFAULT NULL,
      `value_fragment` TINYTEXT DEFAULT NULL,
      `value_initTimestamp` BIGINT(20) DEFAULT NULL,
      `value_lectureID` INT(11) DEFAULT NULL,
      `value_networkState` INT(11) DEFAULT NULL,
      `value_paused` VARCHAR(5) DEFAULT NULL,
      `value_playbackRate` FLOAT DEFAULT NULL,
      `value_prevTime` FLOAT DEFAULT NULL,
      `value_readyState` INT(11) DEFAULT NULL,
      `value_type` VARCHAR(32) DEFAULT NULL,

      `url_parameters` TEXT DEFAULT NULL,
      `url_path` VARCHAR(128) DEFAULT NULL,
      `url_resource` VARCHAR(128) DEFAULT NULL,

      KEY `username_index` (`username`),
      KEY `id_index` (`id`) USING BTREE,
      KEY `user_ip_index` (`user_ip`),
      KEY `path_index` (`url_path`),
      KEY `path_res_index` (`url_resource`,`url_path`),
      KEY `pk` (`id`,`user_ip`,`timestamp`) USING BTREE,
      KEY `time_index` (`timestamp`) USING BTREE
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1
    /*!50100 PARTITION BY KEY (id)
    PARTITIONS 100 */;
    """

    conn.execute(query)

#Discover the table to insert data into
metadata = MetaData()
metadata.reflect(bind=conn)
tbl_coursera_clickstream = metadata.tables["coursera_clickstream"]

for f in files:
    open_fun = open
    if f.endswith(".gz"):
        open_fun = gzip.open

    # the courseid is the filename, but may have a _clickstream_export appended by coursera
    filename = os.path.splitext(basename(f))[0].replace("_clickstream_export", '')
    try:
        fid = convert_sessionid_to_id(filename)  # convert to pk
    except TypeError as e:
        logger.error(
            "Could not convert filename {} session identifier in the database, the best guess was {}.  Perhaps "
            "this course has not been put in the database yet?".format(f, filename))
        sys.exit(-1)

    with open_fun(f) as infile:
        # Start queue
        queue = multiprocessing.Queue()
        if args.output:
            logger.info("Storing output in csv format to file {}".format(args.output))
            dbq = ThreadedCSVQueue(queue, open(args.output,'a'), tbl_coursera_clickstream, batch_size=BATCH_SIZE,
                                   log_to_console=args.verbose, hard_exit_on_failure=False)
        else:
            logger.info("Storing output directly to database.")
            dbq = ThreadedDBQueue(queue, conn, tbl_coursera_clickstream, batch_size=BATCH_SIZE, log_to_console=args.verbose,
                                  hard_exit_on_failure=False)
        dbq.start()

        logger.info("Working with file {}.".format(f))
        line = " "
        while line is not None and line != b'':
            line = infile.readline()
            try:
                js = ujson.loads(line)
                js["id"] = fid
                __json_parser(js, queue)
                __queue_control(queue)
            except Exception as e:
                logger.warn("Exception {} from line '{}'".format(e, line))
                logger.info("line type is {}".format(type(line)))
        # Wait for the db injector to finish processing this file
        dbq.stop()
        logger.info("Waiting for database thread to complete data insertions.")
        while dbq.is_alive():
            time.sleep(1)

#at the very end we now have IP addresses in the clickstream table we
#need to run geolocation on
#todo: this should be refactored into a method call, so much bad here
#command = 'python ./util/geolocate.py  --verbose --sql="SELECT DISTINCT user_ip from coursera_clickstream where ip not in (select distinct(ip) from coursera_geolocate)" --schemas="uselab_mooc"'
#logger.warn("Calling geolocation using command {}".format(command))
#os.system(command)
