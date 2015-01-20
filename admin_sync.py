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

from util.config import *
from core.coursera import Course, Base

import sys
import argparse
from datetime import datetime, timedelta
from time import sleep

from sqlalchemy.orm import sessionmaker
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

'''the amount of time to set the implicit wait too'''
SECONDS_TO_WAIT = 15
'''how many times to try and load an individual course page'''
MAX_RETRIES = 5
'''the amount of time to sleep between individual course reloads'''
MAX_RETRIES_WAIT = 10

login_url = 'https://accounts.coursera.org/signin?mode=signin&post_redirect=%2F'
admin_url = 'https://www.coursera.org/admin/'
session_url = 'https://www.coursera.org/admin/data/sessions/{}'

parser = argparse.ArgumentParser(description='Syncs local database with Coursera Admin website.  This script will '
                                             'update data with respect to existing courses, but will not delete courses'
                                             ' from the database.  To do that, use --clean.')
parser.add_argument('--clean', action='store_true', help='Whether to drop tables in the database or not.  This will '
                                                         'drop the table indiscriminately, proceed with caution.')
parser.add_argument('--verbose', action='store_true', help='Whether to debug log or not')
parser.add_argument('--username', action='store',
                    help="The username to connect to log into the Coursera site with, checks config.properties for "
                         "username if this value does not exist")
parser.add_argument('--password', action='store',
                    help="The password to connect to log into the Coursera site with, checks config.properties for "
                         "username if this value does not exist")
parser.add_argument('--update', action='store_true', help='Whether to update the local database of coursera courses.'
                                                          'This will launch a firefox instance and scrape the web.')
parser.add_argument('--verify', action='store_true', help='Whether to verify if files have been downloaded from '
                                                          'Coursera')
args = parser.parse_args()

logger = get_logger("admin_sync.py", args.verbose)
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

if args.clean:
    query = "DROP TABLE IF EXISTS {}".format(Course.__tablename__)
    try:
        conn.execute(query)
    except Exception as e:
        pass


def load_course_details(course, delay=3):
    print(session_url.format(course['admin_id']))
    browser.get(session_url.format(course['admin_id']))
    # individual course pages
    #give the page a chance to load
    WebDriverWait(browser, SECONDS_TO_WAIT).until(EC.presence_of_element_located(
        (By.CLASS_NAME, "model-admin-control-group-grading_policy_distinction")))
    #todo: wait for some ajax to finish, probably better to be an explicit wait
    sleep(delay)

    bdy = browser.find_element_by_class_name("model-admin-fields")
    course['session_id'] = bdy.find_element_by_xpath('//h1').text.split("/")[0]
    course['course'] = bdy.find_element_by_xpath('div[1]//h3').text
    course['url'] = bdy.find_element_by_xpath('div[2]//a').get_attribute('href')
    year = bdy.find_element_by_name('start_year').find_element_by_xpath(
        ".//option[@selected='selected']").get_attribute('value')
    month = bdy.find_element_by_name('start_month').find_element_by_xpath(
        ".//option[@selected='selected']").get_attribute('value')
    try:
        day = bdy.find_element_by_name('start_day').find_element_by_xpath(
            ".//option[@selected='selected']").get_attribute(
            'value')
    except:
        #its possible there is no day available, really old classes might be like this.
        day = '1'
    course['start'] = datetime(int(year), int(month), int(day))
    end_date = bdy.find_element_by_name('end_date').get_attribute("value")
    try:
        course['end'] = datetime(int(end_date.split('-')[0]), int(end_date.split('-')[1]), int(end_date.split('-')[2]))
    except:
        #it is possible for a course not to have an end date, again, old courses.
        pass

def update_database():
    browser = webdriver.Firefox()
    browser.implicitly_wait(SECONDS_TO_WAIT)
    browser.get(login_url)
    WebDriverWait(browser, SECONDS_TO_WAIT).until(EC.presence_of_element_located((By.ID, "signin-email")))
    browser.find_element_by_id('signin-email').send_keys(username)
    browser.find_element_by_id('signin-password').send_keys(password)
    browser.find_element_by_class_name("coursera-signin-button").submit()

    WebDriverWait(browser, SECONDS_TO_WAIT).until(EC.presence_of_element_located((By.CLASS_NAME, "internal-site-admin")))
    browser.get(admin_url)
    WebDriverWait(browser, SECONDS_TO_WAIT).until(EC.presence_of_element_located((By.CLASS_NAME, "model-admin-table")))
    sleep(5)

    courses = []
    for element in browser.find_elements_by_class_name("internal-site-admin"):
        #ignore links that are not sessions
        if "sessions/" not in element.get_attribute('href'):
            logger.info("URL Not being followed: {}".format(element.get_attribute('href')))
            continue
        logger.debug("This URL will be inspected: {}".format(element.get_attribute('href')))

        course_details = {
            'admin_id': int(element.get_attribute('href').split('/')[-1]),
            'session_id': element.get_attribute('text')[:-1]}
        print("Adding data on {} to list.".format(course_details['session_id']))
        courses.append(course_details)

    #todo: wait for some ajax to finish, probably better to be an explicit wait on some element with a timeout
    sleep(5)

    for c in courses:
        #It's possible that the browser locked up or something on trying to course details, in which cse we will
        #try one more time after cooling down
        for i in range(MAX_RETRIES):
            try:
                load_course_details(c)
                break
            except:
                sleep(MAX_RETRIES_WAIT)
        logger.warn("Course {} would not load.  Tried {} times.".format(c['admin_id'], MAX_RETRIES))

    Base.metadata.create_all(conn)
    Session = sessionmaker(bind=conn)
    session = Session()

    for course in courses:
        session.add(Course(**course))
    session.commit()

def verify_courses():
    Base.metadata.create_all(conn)
    Session = sessionmaker(bind=conn)
    session = Session()

    missing_clickstream=[]
    missing_sql=[]
    missing_intent=[]
    missing_demographics=[]
    for course in session.query(Course):
        # todo: Eventually this should use the end time of the course, but that is unreliable on old courses.
        if course.start is not None:
            if datetime.today() - course.start > timedelta(days=106):
                if not course.has_clickstream():
                    missing_clickstream.append(course)
                if not course.has_sql():
                    missing_sql.append(course)
                if not course.has_intent():
                    missing_intent.append(course)
                if not course.has_demographics():
                    missing_demographics.append(course)

    print("The following courses are missing clickstream files: ")
    for course in missing_clickstream:
        print(course.session_id, end=', ')
    print("\nThe following courses are missing sql files: ")
    for course in missing_sql:
        print(course.session_id, end=', ')
    print("\nThe following courses are missing intent files: ")
    for course in missing_intent:
        print(course.session_id, end=', ')
    print("\nThe following courses are missing demographics files: ")
    for course in missing_demographics:
        print(course.session_id, end=', ')
    print("\n\nTo request up to date files please see https://docs.google.com/a/umich.edu/forms/d/1VI9G_0uU2tr7-0hFNINOl8Gi79Tw0Dme4BsilLCixgM/viewform")

# todo there must be a slicker argsparser way to do this
if not args.update and not args.verify:
    print("You must use either --update or --verify.")
else:
    if args.update:
        update_database()
    if args.verify:
        verify_courses()