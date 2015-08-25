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
from sqlalchemy.sql import exists, select
import csv

parser = argparse.ArgumentParser(description='Gets email addresses from users for the last n days')
parser.add_argument('--days', help='How many days from now, e.g. 180 for 6 months. Default is 180', type=int,
                    default=180)
parser.add_argument('--verbose', action='store_true', help='Whether to debug log or not')

args = parser.parse_args()
logger = get_logger("email_list.py", args.verbose)
conn = get_db_connection()

Base.metadata.create_all(conn)
Session = sessionmaker(bind=conn)
session = Session()

schemas = []
for course in session.query(Course):
    # todo: this is a bit of a hack because it is postgresql specific.  Probably a better way to do this which is
    # more general?
    if (conn.scalar(
            "SELECT exists(SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{}');".format(
                course.schema_name()))):
        schemas.append(course.schema_name())
    else:
        print("Course {} in schemas {} is being ignored as it does not exist in database.".format(course.session_id,
                                                                                                  course.schema_name()))

# iterate through schemas building up a userid list
query = """
SELECT
	h.user_id
FROM
	{}.users u,
	{}.hash_mapping h
WHERE
	h.session_user_id=u.session_user_id AND
	last_access_time > extract(epoch
FROM
	getdate()-{});
"""
users = set()
# get a new connection, since we don't want the session manager managing this one
conn = get_db_connection()
for schema in schemas:
    try:
        # todo: there must be a better way of telling sqlalchemy to change proxies into concrete values
        for person in conn.execute(query.format(schema, schema, args.days)).fetchall():
            users.update(person)
    except Exception as e:
        print(e)

        print("Problem with schema {}".format(schema))

# todo: this is a shitty way to turn the elements of a set into a query
query="""
select email_address, full_name from public.coursera_pii where coursera_user_id in ({});""".format(str(users)).replace("{","").replace("}","")
rs=conn.execute(query)

with open('out.csv', 'w') as csvfile:
    writer = csv.writer(csvfile)
    for row in rs:
        writer.writerow(list(row))