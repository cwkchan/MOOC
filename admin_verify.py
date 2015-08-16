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

from core.coursera import *
from util.coursera_db import *

import argparse
from datetime import datetime, timedelta

from sqlalchemy.orm import sessionmaker


parser = argparse.ArgumentParser(description='Verifies that all the required files from coursera are present in the respective folders')
parser.add_argument('--verbose', action='store_true', help='Whether to debug log or not')

args = parser.parse_args()
logger = get_logger("admin_verify.py", args.verbose)
conn = get_db_connection()

def verify_courses():
    Base.metadata.create_all(conn)
    Session = sessionmaker(bind=conn)
    session = Session()

    missing_clickstream=[]
    missing_sql=[]
    missing_intent=[]
    missing_demographics=[]
    missing_pii=[]

    for course in session.query(Course):
        # todo: Eventually this should use the end time of the course, but that is unreliable on old courses.
        if course.start_date is not None:
            if datetime.today().date() - course.start_date > timedelta(days=106):
                if not course.has_clickstream():
                    missing_clickstream.append(course)
                if not course.has_sql():
                    missing_sql.append(course)
                if not course.has_intent():
                    missing_intent.append(course)
                if not course.has_demographics():
                    missing_demographics.append(course)
                if not course.has_pii():
                    missing_pii.append(course)

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
    print("\nThe following courses are missing pii files: ")
    for course in missing_pii:
        print(course.session_id, end=', ')
    print("\n\nTo request up to date files please see https://docs.google.com/a/umich.edu/forms/d/1VI9G_0uU2tr7-0hFNINOl8Gi79Tw0Dme4BsilLCixgM/viewform")


verify_courses()