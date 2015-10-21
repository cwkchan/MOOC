# Copyright (C) 2013  The Regents of the University of Michigan
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see [http://www.gnu.org/licenses/].

from util.coursera_web import *
from util.coursera_db import *
import sys
import argparse
import re

from pyvirtualdisplay import Display
from sqlalchemy.orm import sessionmaker

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


parser = argparse.ArgumentParser(description='Downloads the assignments from the specified course')
parser.add_argument('--schema', action='store',
                    help="The schema to create the tables in. It is present in the filename in brackets."
                         "Please use the accurate schema. Currently the schema name is not being verified")
parser.add_argument('--assignment', action='store',
                    help="The assignment to download. PAss all to download all assignments.")
parser.add_argument('--verbose', action='store_true', help='Whether to debug log or not')
parser.add_argument('--username', action='store',
                    help="The username to connect to log into the Coursera site with, checks config.properties for "
                         "username if this value does not exist")
parser.add_argument('--password', action='store',
                    help="The password to connect to log into the Coursera site with, checks config.properties for "
                         "username if this value does not exist")

args = parser.parse_args()

if args.assignment is None:
    print("You must specify a assignment number to download. pass all to download all assignments.")
    sys.exit(-1)
else:
    if args.assignment == 'all':
        all = True
    else:
        assignment = 'Assignment {} (quiz)'.format(args.assignment)
        all = False

print(assignment)
if args.schema is None:
    print("You must specify a schema.Please use the accurate schema. Currently the schema name is not being verified")
    sys.exit(-1)

schema = args.schema


logger = get_logger("admin_sync.py", args.verbose)
conn = get_db_connection()

username, password = username_and_password_exist(args)
url = "https://class.coursera.org/introfinance-004/data/export/csv_quiz_responses"
# display = Display(visible=0, size=(800, 600))
# display.start()



def load_course_details():
    browser = webdriver.Firefox(firefox_profile=get_download_profile())
    login_coursera_website(browser,username,password)
    WebDriverWait(browser, SECONDS_TO_WAIT).until(EC.presence_of_element_located((By.ID, "rendered-content")))
    browser.get(url)
    files = []

    el = browser.find_element_by_name('quiz_id')
    for option in el.find_elements_by_tag_name('option'):
        if re.match(r"Assignment*".format(assignment), option.text):
            files.append((option.text))
    if all == True:
        for option in browser.find_element_by_name('quiz_id').find_elements_by_tag_name('option'):
            if option.text in files:
                print(option.text)
                download_file(option, browser)
    else:
        for option in browser.find_element_by_name('quiz_id').find_elements_by_tag_name('option'):
            if option.text == assignment:
                print(option.text)
                download_file(option, browser)

    #display.stop()

def download_file(element, browser):
    element.click()
    browser.find_element_by_name("include_cuid").click()
    browser.find_element_by_name("submit").click()
    browser.find_element_by_link_text("Refresh").click()
    while (check_exists_by_xpath(".//*[@id='spark']/table/tbody/tr[1]/td[5]/a[1]", browser) != True):
        browser.find_element_by_link_text("Refresh").click()
        WebDriverWait(browser, SECONDS_TO_WAIT)
    browser.find_element_by_xpath(".//*[@id='spark']/table/tbody/tr[1]/td[5]/a[1]").click()

load_course_details()