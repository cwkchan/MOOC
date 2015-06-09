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

import argparse
import pandas
from sqlalchemy.orm import sessionmaker
from util.config import *
from core.coursera import Course, Base

'''The choices from the coursera intent survey are:
0: "... mastering the course material by working through the exercises and earning a certificate."
1: "... learning the course material mainly by watching most of the lectures."
2: "None of the above. I'm just checking out the course for now."
3: "No Answer"
'''

parser = argparse.ArgumentParser(description='Import coursera intent files into the database.  Will check which courses'
                                             'have not have files loaded into the DB and attempt to load them.')
parser.add_argument('--clean', action='store_true', help='Whether to drop the table in the database to load from scratch or not')
parser.add_argument('--verbose', action='store_true', help='If this flag exists extended logging will be on')
args = parser.parse_args()

logger = get_logger("load_intent.py", args.verbose)
conn = get_connection()

if args.clean:
    query = "DROP TABLE IF EXISTS coursera_intent"
    try:
        conn.execute(query)
    except:
        pass

query = """CREATE TABLE IF NOT EXISTS `coursera_intent` (
            `intent_id` INT NOT NULL AUTO_INCREMENT,
            `user_id` INT NOT NULL,
            `session_id` VARCHAR(255) NOT NULL,
            `submission_ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            `choice_id` INT NOT NULL,
            PRIMARY KEY (`intent_id`)
            ) ; """

conn.execute(query)

Base.metadata.create_all(conn)
Session = sessionmaker(bind=conn)
session = Session()

metadata = MetaData()
metadata.reflect(bind=conn)
tbl_coursera_intent = metadata.tables["coursera_intent"]

def __intent_loaded(course):
    result = conn.scalar("SELECT count(*) FROM coursera_intent where session_id = '{}'".format(course.session_id))
    if result != 0:
        return True
    return False


for course in session.query(Course):
    if not __intent_loaded(course) and course.has_intent():
         # for some reason the exports from coursera end with a count of the number of rows in file.
        df = pandas.read_csv(course.get_intent_filename(), skipfooter=1)
        df["session_id"]=course.session_id
        df.to_sql('coursera_intent', conn, if_exists='append', index=False)
