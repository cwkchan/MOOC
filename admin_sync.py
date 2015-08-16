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
from core.coursera import Course, Base
from util.coursera_db import *
import sys
import argparse
from datetime import datetime
from time import sleep

from pyvirtualdisplay import Display
from sqlalchemy.orm import sessionmaker
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


parser = argparse.ArgumentParser(description='Syncs local database with Coursera Admin website.  This script will '
                                             'update data with respect to existing courses, but will not delete courses'
                                             ' from the database.  To do that, use --clean.')
parser.add_argument('--clean', action='store_true', help="This is a limitation of the script and it has to reload the database at every run "
                                             "As of now, the script is unable to accomodate updates."
                                             "This needs to be provided for the script to proceed")
parser.add_argument('--verbose', action='store_true', help='Whether to debug log or not')
parser.add_argument('--username', action='store',
                    help="The username to connect to log into the Coursera site with, checks config.properties for "
                         "username if this value does not exist")
parser.add_argument('--password', action='store',
                    help="The password to connect to log into the Coursera site with, checks config.properties for "
                         "username if this value does not exist")

args = parser.parse_args()

if not args.clean:
    print("\nPlease use the --clean as an argument. It will delete all the data from coursera_index and reload it"
          "\nThis is a limitation of the script and it has to reload the database at every run "
          "\nAs of now, the script is unable to accomodate updates."
          "\nThis needs to be provided for the script to proceed")
    sys.exit(1)

logger = get_logger("admin_sync.py", args.verbose)
conn = get_db_connection()

username, password = username_and_password_exist(args)

display = Display(visible=0, size=(800, 600))
display.start()

def create_index():
    query = '''DROP TABLE IF EXISTS {}'''.format(Course.__tablename__)
    try:
        conn.execute(query)
    except Exception as e:
        pass

    query= ('CREATE TABLE IF NOT EXISTS coursera_index(\n'
            '  session_id VARCHAR(50),\n'
            '  admin_id INTEGER NOT NULL,\n'
            '  course VARCHAR(255),\n'
            '  instructor VARCHAR(255),\n'
            '  start_date DATE,\n'
            '  end_date DATE,\n'
            '  duration INTEGER,\n'
            '  enrollment VARCHAR(255),\n'
            '  url VARCHAR(255),\n'
            '  allow_signature INTEGER,\n'
            '  PRIMARY KEY(admin_id)\n'
            ');')

    try:
        conn.execute(query)
    except Exception as e:
        logger.fatal("Failed to create new table for index")


def load_course_details(course, browser, delay=3):
    #print(SESSION_URL.format(course['admin_id']))
    browser.get(SESSION_URL.format(course['admin_id']))
    # individual course pages
    #give the page a chance to load
    WebDriverWait(browser, SECONDS_TO_WAIT).until(EC.presence_of_element_located(
        (By.CLASS_NAME, "model-admin-control-group-grading_policy_distinction")))
    #todo: wait for some ajax to finish, probably better to be an explicit wait
    sleep(delay)

    try:
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
        course['start_date'] = datetime(int(year), int(month), int(day))
        end_date = bdy.find_element_by_name('end_date').get_attribute("value")
        try:
            course['end_date'] = datetime(int(end_date.split('-')[0]), int(end_date.split('-')[1]), int(end_date.split('-')[2]))
        except:
            #it is possible for a course not to have an end date, again, old courses.
            pass
    except:
        #it is possible that one or more of these elements do not exist
        #consider this malformed and just return None
        #todo: not sure what the implications are on trying to put it into the db.
        pass


def update_database():
    browser = webdriver.Firefox()
    login_coursera_website(browser, username, password)
    sleep(3)
    browser.get(ADMIN_URL)
    WebDriverWait(browser, SECONDS_TO_WAIT).until(EC.presence_of_element_located((By.CLASS_NAME, "model-admin-table")))
    sleep(5)

    courses = []
    blacklist =(get_properties()['course_blacklist'])

    for element in browser.find_elements_by_class_name("internal-site-admin"):
        #ignore links that are not sessions
        if "sessions/" not in element.get_attribute('href'):
            logger.info("URL Not being followed: {}".format(element.get_attribute('href')))
            continue
        logger.debug("This URL will be inspected: {}".format(element.get_attribute('href')))

        course_details = {
            'admin_id': int(element.get_attribute('href').split('/')[-1]),
            'session_id': element.get_attribute('text')[:-1]}

        if course_details['session_id'] not in blacklist:
            print("Adding data on {} to list.".format(course_details['session_id']))
            courses.append(course_details)
        else :
            print("Skipping the blacklisted course {}.".format(course_details['session_id']))


    #todo: wait for some ajax to finish, probably better to be an explicit wait on some element with a timeout
    sleep(5)

    for c in courses:
        #It's possible that the browser locked up or something on trying to course details, in which cse we will
        #try one more time after cooling down
        for i in range(MAX_RETRIES):
            try:
                load_course_details(c, browser)
                break
            except Exception as e:
                sleep(MAX_RETRIES_WAIT)
        if i >= MAX_RETRIES:
            logger.warn("Course {} would not load.  Tried {} times.".format(c['admin_id'], MAX_RETRIES))

    Base.metadata.create_all(conn)
    Session = sessionmaker(bind=conn)
    session = Session()

    for course in courses:
        session.add(Course(**course))
    session.commit()

    browser.quit()
    display.stop()

create_index()
update_database()
