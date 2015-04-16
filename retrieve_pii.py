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
from io import StringIO

from util.config import *
from core.coursera import Course, Base
import http.cookiejar
import urllib.request
import sys
import argparse
from datetime import datetime, timedelta
from time import sleep
from core.coursera import Course, Base
from sqlalchemy.orm import sessionmaker
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas

'''the amount of time to set the implicit wait too'''
SECONDS_TO_WAIT = 15
'''how many times to try and load an individual course page'''
MAX_RETRIES = 5
'''the amount of time to sleep between individual course reloads'''
MAX_RETRIES_WAIT = 10

login_url = 'https://accounts.coursera.org/signin?mode=signin&post_redirect=%2F'
admin_url = 'https://www.coursera.org/admin/'
session_url = 'https://www.coursera.org/admin/data/sessions/{}'

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
    if args.username is None:
        username = get_properties()['username']
    else:
        username = args.username
    if args.password is None:
        password = get_properties()['password']
    else:
        password = args.password
    if username is None or password is None:
        raise Exception("No username or password found.")
except Exception as e:
    print("Error: No username or password found.")
    parser.print_help()
    sys.exit(1)

browser = webdriver.Firefox()
browser.implicitly_wait(SECONDS_TO_WAIT)
browser.get(login_url)
WebDriverWait(browser, SECONDS_TO_WAIT).until(EC.presence_of_element_located((By.ID, "signin-email")))
browser.find_element_by_id('signin-email').send_keys(username)
browser.find_element_by_id('signin-password').send_keys(password)
browser.find_element_by_class_name("coursera-signin-button").submit()

sleep(2)

cj = http.cookiejar.CookieJar()
for item in browser.get_cookies():
    c = http.cookiejar.Cookie(0, item['name'], item['value'], None, None, item['domain'], None, None,
                              item['path'], None, item['secure'], item['expiry'], None, None, None, None)
    cj.set_cookie(c)
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))

Session = sessionmaker(bind=conn)
session = Session()
pii = pandas.DataFrame()
for course in session.query(Course):
    try:
        print("Working with {}".format(course.session_id))
        f = opener.open('https://class.coursera.org/{}/data/export/pii_download'.format(course.session_id))
        lines = f.read().decode('utf-8')
        pii = pii.append(pandas.read_csv(StringIO(lines)))
    except Exception as e:
        print("Receive an error {} for URL: {}".format(e,'https://class.coursera.org/{}/data/export/pii_download'.format(course.session_id)))

pii.to_csv("pii.csv",index=False)

"""
session.
for cookie in browser.get_cookies():
    print(cookie)

import requests
import http.cookiejar


print("trying get")
browser.get('https://class.coursera.org/fantasysf-2012-001/data/export/pii_download')
"""