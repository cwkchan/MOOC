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

from util.coursera_files import *
from core.coursera import Course, Base

import argparse
import pandas
from sqlalchemy.orm import sessionmaker

logger = get_logger("load_pii")

parser = argparse.ArgumentParser(description='Import coursera pii files into the database.  Will check which courses'
                                             'have not have files loaded into the DB and attempt to load them.')
parser.add_argument('--clean', action='store_true', help='Whether to drop tables in the database or not')
parser.add_argument('--verbose', action='store_true', help='If this flag exists extended logging will be on')
args = parser.parse_args()

logger = get_logger("load_pii.py", args.verbose)
conn = get_db_connection()
s3_path = (get_properties().get('s3_path', None) + 'pii/')


if args.clean:
    query = "DROP TABLE IF EXISTS coursera_pii"
    try:
        conn.execute(query)
    except:
        pass

query = (""
         "CREATE TABLE IF NOT EXISTS coursera_pii ("
         "coursera_user_id INTEGER NOT NULL,"
         "access_group VARCHAR(255) NOT NULL,"
         "email_address VARCHAR(255) DEFAULT NULL,"
         "full_name VARCHAR(1024) DEFAULT NULL,"
         "last_access_ip VARCHAR(255) DEFAULT NULL,"
         "deleted INTEGER DEFAULT 0,"
         "session_id VARCHAR(255) NOT NULL,"
         "PRIMARY KEY (coursera_user_id, session_id)"
         ");")

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
        df.to_csv('/tmp/{}.csv'.format(course.session_id),header=False, sep='|', index=False, quoting=csv.QUOTE_ALL )

        file_name = '/tmp/{}.csv'.format(course.session_id)
        try:
            pass #upload_s3_file(file_name, 'pii')
        except:
            logger.exception("This file : {} could not be uploaded to S3. Please update manually".format(file_name))
            logger.exception(traceback.format_exc(limit=None))

        path = (s3_path + '{}.csv'.format(course.session_id))
        print(path)

        try:
            pass #copy_s3_to_redshift(conn, path, 'coursera_pii',schema=None, delim="|", error=0, ignoreheader=0)

        except:
            logger.exception("This table : {} could not be loaded from the file : {}. Please check pgcatalog.stl_load_errors".format('coursera_pii',path ))
            logger.exception(traceback.format_exc(limit=None))

        # else:
        #     os.remove(file_name)

