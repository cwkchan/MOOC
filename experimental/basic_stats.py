#    Copyright (C) 2013  The Regents of the University of Michigan
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
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
import pandas as pd
import matplotlib.pyplot as plt
from util.config import *

parser = argparse.ArgumentParser(description='Generate engagement for each user across course blocks.')
parser.add_argument('--schemas', help='An optional list of the schemas to create tables from', required=False)
args = parser.parse_args()

conn = get_connection()

if (args.schemas != None):
    schemas = args.schemas.split(",");
else:
    # Get a list of the databases to run this query over
    query = """SELECT id from coursera_index;"""
    schemas = []
    for row in conn.execute(query):
        schemas.append(row[0].encode('ascii','ignore'))

for schema_name in schemas:
    try:
        print "Working with schema " + str(schema_name)
        schemaconn = get_connection(schema_name)
    except Exception, e:
        # Chances are this database doesn't exist, move onto the next schema
        print 'Error accessing '+str(schema_name)+' with exception '+str(e)
        continue

    query = """SELECT session_user_id,normal_grade,achievement_level FROM course_grades WHERE normal_grade>0"""
    course_grades = pd.io.sql.read_frame(query, schemaconn.raw_connection())
    mean = float(course_grades.mean())
    std = float(course_grades.std())
    print 'Mean: '+str(mean)
    print 'SD: '+str(std)

    grades = course_grades['normal_grade'].as_matrix()
    plt.hist(grades, 20)
    plt.show()

