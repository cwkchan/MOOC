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
import scipy.stats
import numpy as np
import matplotlib.pyplot as plt
import time
from util.config import *

parser = argparse.ArgumentParser(description='Generate engagement for each user across course blocks.')
parser.add_argument('--schemas', help='An optional list of the schemas to create tables from', required=False)
args = parser.parse_args()

conn = get_connection()

if (args.schemas != None):
    schemas = args.schemas.split(",")
    query = """SELECT session_id,course,start,duration FROM coursera_index WHERE """
    for schema_name in schemas:
        query += 'session_id="'+schema_name+'" OR '
    query = query[:-4]
    coursera_index = pd.io.sql.read_frame(query, conn.raw_connection(), index_col='session_id')
else:
    # Get a list of the databases to run this query over
    query = """SELECT session_id from coursera_index;"""
    schemas = []
    for row in conn.execute(query):
        schemas.append(row[0].encode('ascii','ignore'))
    query = """SELECT session_id,course,start,duration FROM coursera_index"""
    coursera_index = pd.io.sql.read_frame(query, conn.raw_connection(), index_col='session_id')


for schema_name in schemas:
    try:
        print "Working with schema " + str(schema_name)
        schemaconn = get_connection(schema_name)
    except Exception, e:
        # Chances are this database doesn't exist, move onto the next schema
        print 'Error accessing '+str(schema_name)+' with exception '+str(e)
        continue

    try:
        query = """SELECT COUNT(DISTINCT(session_user_id))
                   FROM   users;"""
        registered = pd.io.sql.read_frame(query, schemaconn.raw_connection()).ix[0,0]

        query = """SELECT COUNT(DISTINCT(session_user_id))
                   FROM   lecture_submission_metadata;"""
        watched_lecture = pd.io.sql.read_frame(query, schemaconn.raw_connection()).ix[0,0]

        query = """SELECT COUNT(DISTINCT(session_user_id))
                   FROM   course_grades
                   WHERE  normal_grade>0;"""
        attempted_assignment = pd.io.sql.read_frame(query, schemaconn.raw_connection()).ix[0,0]

        query = """SELECT COUNT(DISTINCT(session_user_id))
                   FROM   course_grades
                   WHERE  achievement_level='normal'
                      OR  achievement_level='distinction';"""
        statement_of_accomplishment = pd.io.sql.read_frame(query, schemaconn.raw_connection()).ix[0,0]

        print '  Users:'
        print '  '+str(registered)+' registered, '+str(watched_lecture)+' active (i.e., accessed a lecture) ('+str(int(100*1.0*watched_lecture/registered))+'% of registered users)'
        print '  '+str(attempted_assignment)+' earned a grade >0 ('+str(int(100*1.0*attempted_assignment/watched_lecture))+'% of active users)'
        print '  '+str(statement_of_accomplishment)+' received a Statement of Accomplishment ('+str(int(100*1.0*statement_of_accomplishment/watched_lecture))+'% of active users)'
        print ''
    except:
        pass

    try:
        start_date = coursera_index.loc[schema_name,'start']
        start_timestamp = int(time.mktime(time.strptime(start_date, '%m/%d/%Y'))) - time.timezone
        duration = int(coursera_index.loc[schema_name,'duration'])
        timestamps = []
        for i in range(0,duration+1):
            timestamps.append( start_timestamp + (i*604800) )

        lecture_submissions = []
        query = """SELECT submission_time
                   FROM   lecture_submission_metadata
                   WHERE  submission_time>=%s
                     AND  action='view'""" % str(start_timestamp)
        lecture_submissions.append(pd.io.sql.read_frame(query, schemaconn.raw_connection()))
        query = """SELECT submission_time
                   FROM   lecture_submission_metadata
                   WHERE  submission_time>=%s
                     AND  action='download'""" % str(start_timestamp)
        lecture_submissions.append(pd.io.sql.read_frame(query, schemaconn.raw_connection()))

        # Plot lecture submissions over time, colored by views/downloads
        plt.figure()
        for timestamp in timestamps:
            plt.axvline(x=timestamp, color='#999999')
        n, bins, patches = plt.hist(lecture_submissions, 100, histtype='bar', stacked=True, color=['#0d57aa','#ffcb0b'], label=['Views','Downloads'])
        plt.legend()
        plt.title('Lecture Submissions Over Time')
        plt.xlabel('Date')
        plt.ylabel('Count')

    except Exception, e:
        print e

    try:
        query = """SELECT session_user_id,normal_grade,achievement_level FROM course_grades WHERE normal_grade>0"""
        course_grades = pd.io.sql.read_frame(query, schemaconn.raw_connection())
        grades_by_time = []

        for i in range(len(timestamps)):
            start = timestamps[i]
            if i < len(timestamps) - 1:
                end = timestamps[i+1]
                query = """SELECT course_grades.session_user_id
                                , normal_grade
                                , last_submission_time
                           FROM   course_grades
                                  LEFT JOIN
                                     (SELECT session_user_id
                                           , MAX(submission_time) AS last_submission_time
                                      FROM   lecture_submission_metadata
                                      GROUP BY session_user_id) lecture_submissions
                                  ON course_grades.session_user_id=lecture_submissions.session_user_id
                           WHERE normal_grade>0
                             AND last_submission_time>=%s
                             AND last_submission_time<%s""" % (start, end)
            else:
                query = """SELECT course_grades.session_user_id
                                , normal_grade
                                , last_submission_time
                           FROM   course_grades
                                  LEFT JOIN
                                     (SELECT session_user_id
                                           , MAX(submission_time) AS last_submission_time
                                      FROM   lecture_submission_metadata
                                      GROUP BY session_user_id) lecture_submissions
                                  ON course_grades.session_user_id=lecture_submissions.session_user_id
                           WHERE normal_grade>0
                             AND last_submission_time>=%s""" % start
            grades_by_time.append(pd.io.sql.read_frame(query, schemaconn.raw_connection())['normal_grade'].values)

        try:
            query = """SELECT MIN(normal_grade) AS accomplishment_grade
                       FROM   course_grades
                       WHERE  achievement_level='normal'"""
            accomplishment_grade = pd.io.sql.read_frame(query, schemaconn.raw_connection())
        except Exception, e:
            print e

        # Plot course grade distribution, colored by last lecture submission
        colors = []
        labels = []
        time_len = len(grades_by_time)
        for i in range(time_len):
            colors.append((float(i)/time_len,0.2,1-float(i)/time_len))
            labels.append('Week '+str(i+1))
        labels[time_len-1] = 'Beyond'
        plt.figure()
        n, bins, patches = plt.hist(grades_by_time, 100, histtype='bar', stacked=True, color=colors, label=labels)
        plt.axvline(x=accomplishment_grade.ix[0,0], color='#999999')
        plt.legend(title='Last lecture view')
        plt.title('Distribution of Course Grades')
        plt.xlabel('Grade')
        plt.ylabel('Count')

        d,p = scipy.stats.kstest(course_grades['normal_grade'],'norm')
        mean = float(course_grades.mean())
        std = float(course_grades.std())
        print '  Course grades:'
        print '  mean='+str(mean)+', sd='+str(std)
        print '  Kolmogorov-Smirnov: D='+str(d)+', p='+str(p)
        print ''

    except Exception, e:
        print e
        continue

    plt.show()