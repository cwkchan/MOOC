# Copyright (C) 2013  The Regents of the University of Michigan
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see [http://www.gnu.org/licenses/].

from util.coursera_web import *
from core.coursera import Course

import sys
from io import StringIO
import argparse
from time import sleep

from selenium import webdriver
from sqlalchemy.orm import sessionmaker

import pandas

parser = argparse.ArgumentParser(description='')
parser.add_argument('--verbose', action='store_true', help='Whether to debug log or not')
parser.add_argument('--username', action='store',
                    help="The username to connect to log into the Coursera site with, checks config.properties for "
                         "username if this value does not exist")
parser.add_argument('--password', action='store',
                    help="The password to connect to log into the Coursera site with, checks config.properties for "
                         "username if this value does not exist")

args = parser.parse_args()
logger = get_logger("retrieve_pii.py", args.verbose)
conn = get_connection()

try:
    username, password = username_and_password_exist(args)
except Exception as e:
    print("Error: No username or password found.")
    parser.print_help()
    sys.exit(1)

browser = webdriver.Firefox()
login_coursera_website(browser, username,password)
sleep(2)

opener=build_coursera_opener(browser)

Session = sessionmaker(bind=conn)
session = Session()

for course in session.query(Course):
    try:
        print("Working with {}".format(course.session_id))
        pii = pandas.DataFrame()
        f = opener.open('https://class.coursera.org/{}/data/export/pii_download'.format(course.session_id))
        lines = f.read().decode('utf-8')
        pii = pii.append(pandas.read_csv(StringIO(lines)))
        pii.to_csv("{}/{}.csv".format((get_properties()['pii']), course.session_id),index=False)
    except Exception as e:
        print("Received an error {} for URL: {}".format(e,'https://class.coursera.org/{}/data/export/pii_download'.format(course.session_id)))

browser.close()
