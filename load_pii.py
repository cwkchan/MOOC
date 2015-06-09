# Copyright (C) 2013 The Regents of the University of Michigan
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
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

import argparse

import pandas
from sqlalchemy.orm import sessionmaker

"""The personally identifiable information files contain the following :
full_name
email_address
coursera_user_id
session_user_id
forum_user_id """


parser = argparse.ArgumentParser(description='Import coursera pii files into the database.  Will check which courses'
                                             'have not have files loaded into the DB and attempt to load them.')
parser.add_argument('--clean', action='store_true', help='Whether to drop tables in the database or not')
parser.add_argument('--verbose', action='store_true', help='If this flag exists extended logging will be on')
args = parser.parse_args()

logger = get_logger("load_pii.py", args.verbose)
conn = get_connection()

if args.clean:
    query = "DROP TABLE IF EXISTS coursera_pii"
    try:
        conn.execute(query)
    except:
        pass

query = """CREATE TABLE IF NOT EXISTS `coursera_pii` (
        `pii_id` INT NOT NULL AUTO_INCREMENT NOT NULL,
        `full_name` VARCHAR(255) NOT NULL,
        `email_address` VARCHAR(255) NOT NULL,
        `coursera_user_id` INT NOT NULL,
        `session_user_id` VARCHAR(255) NOT NULL,
        `forum_user_id` VARCHAR(255) NOT NULL,
        `session_id` VARCHAR(255) NOT NULL,
        PRIMARY KEY (`pii_id`));
        """
conn.execute(query)

Base.metadata.create_all(conn)
Session = sessionmaker(bind=conn)
session = Session()

metadata = MetaData()
metadata.reflect(bind=conn)
tbl_coursera_pii = metadata.tables["coursera_pii"]

def __pii_loaded(course):
    result = conn.scalar("SELECT count(*) FROM coursera_pii where session_id = '{}'".format(course.session_id))
    if result != 0:
        return True
    return False

for course in session.query(Course):
    if not __pii_loaded(course) and course.has_pii():
        df = pandas.read_csv(course.get_pii_filename())
        df["session_id"]=course.session_id
        df.to_sql('coursera_pii', conn, if_exists='append', index=False)
