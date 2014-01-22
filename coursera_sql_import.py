#    Copyright (C) 2013  The Regents of the University of Michigan
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
from os import listdir
from util.config import *
import signal
import sys


parser = argparse.ArgumentParser(description='Import Coursera SQL dumps into database.')
parser.add_argument('--clean', action='store_true',
                    help='Whether to drop tables that already exist in the database or not')
parser.add_argument('--username', action='store', required='True', help="The username to connect to mysql with")
parser.add_argument('--password', action='store', required='True', help="The password to connect to mysql with")
parser.add_argument('--hostname', action='store', required='False', default="localhost",
                    help="The mysql host to connect to")
parser.add_argument('--verbose', action='store_true', help='Whether to debug log or not')
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--csvs', help='A list of csv files to create tables from')
group.add_argument('--dir', help='A directory with CSV files in it')
args = parser.parse_args()

logger = get_logger("coursera_sql_import.py", args.verbose)
conn = get_connection()

def signal_handler(signal, frame):
    sys.exit(0)

def parse_course_name_from_filename(filename, reformat_early=True):
    """Returns the best guess at convering the filename into a course name.  E.g. the file
    Securing Digital Democracy (digitaldemocracy-002)_SQL_unanonymizable.sql would be turned into
    digitaldemocracy-002.  If reformat_early=True then the early course identifiers from Coursera
    that have dates in them will be stripped and if they do not have a -001 appended this wil  be done."""
    try:
        if reformat_early:
            nodates = filename.split("(")[1].split(")")[0].replace("2012-", "")
            if nodates[-3:-1] != "00":
                nodates += "-001"
            return nodates, filename
        return filename.split("(")[1].split(")")[0], filename
    except:
        logger.info("Unknown file {}, ignoring".format(filename))
        return None

#determine list of courses we are dealing with
files = []
if args.dir is not None:
    files = listdir(args.dir)
    files = map(lambda x: args.dir + x, files)
else:
    files = args.csvs.split(',')

files = filter(None, map(parse_course_name_from_filename, files))
schemas = (sorted(set(zip(*files)[0])))

#drop old databases if expected
if args.clean:
    query = "DROP SCHEMA IF EXISTS `{}`"
    for schema in schemas:
        try:
            conn.execute(query.format(schema))
        except Exception as e:
            logger.warn("Failed to drop schema {}".format(e))

#create new databases
query = "CREATE SCHEMA `{}`"
for schema in schemas:
    try:
        conn.execute(query.format(schema))
    except Exception as e:
        logger.warn("Failed to create schema {}".format(e))

#populate with data
signal.signal(signal.SIGINT, signal_handler)
for fil in files:
    logger.info("Inserting data into {} from file {}".format(fil[0], fil[1]))
    filename = fil[1].replace(" ", "\ ").replace("(", "\(").replace(")", "\)")
    cmd = "mysql -u {} -p{} -h {} {} < {}".format(args.username, args.password, args.hostname, fil[0], filename)
    os.system(cmd)