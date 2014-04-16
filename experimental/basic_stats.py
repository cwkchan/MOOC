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
import sys, os

sys.path.append(os.path.abspath(".."))
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

    # Engagement numbers
    try:
        query = """SELECT COUNT(DISTINCT(session_user_id))
                   FROM   users;"""
        registered = pd.io.sql.read_frame(query, schemaconn.raw_connection()).ix[0,0]

        query = """SELECT COUNT(DISTINCT(session_user_id))
                   FROM   lecture_submission_metadata;"""
        active = pd.io.sql.read_frame(query, schemaconn.raw_connection()).ix[0,0]

        query = """SELECT COUNT(DISTINCT(session_user_id))
                   FROM   course_grades
                   WHERE  normal_grade>0;"""
        graded = pd.io.sql.read_frame(query, schemaconn.raw_connection()).ix[0,0]

        query = """SELECT COUNT(DISTINCT(session_user_id))
                   FROM   course_grades
                   WHERE  achievement_level='normal'
                      OR  achievement_level='distinction';"""
        statement_of_accomplishment = pd.io.sql.read_frame(query, schemaconn.raw_connection()).ix[0,0]

        print '  '+str(registered)+' registered users'
        print '  '+str(int(100*1.0*active/registered))+'% of registered users were active (i.e., accessed a lecture) ('+str(active)+' total)'
        print '  '+str(int(100*1.0*graded/active))+'% of active users were graded ('+str(graded)+' total)'
        print '  '+str(int(100*1.0*statement_of_accomplishment/active))+'% of active users earned a Statement of Accomplishment ('+str(statement_of_accomplishment)+' total)'
        print ''
    except Exception, e:
        pass

    # Lecture submissions
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
                     AND  action='view';""" % str(start_timestamp)
        lecture_submissions.append(pd.io.sql.read_frame(query, schemaconn.raw_connection())['submission_time'].values)
        query = """SELECT submission_time
                   FROM   lecture_submission_metadata
                   WHERE  submission_time>=%s
                     AND  action='download';""" % str(start_timestamp)
        lecture_submissions.append(pd.io.sql.read_frame(query, schemaconn.raw_connection())['submission_time'].values)
        end_timestamp = max([max(lecture_submissions[0]), max(lecture_submissions[1])])
        days = np.ceil(1.0*(end_timestamp - start_timestamp)/86400)
        weeks = np.ceil(1.0*(end_timestamp - start_timestamp)/604800)

        # Plot lecture submissions over time, colored by views/downloads
        plt.figure()
        plt.subplot(2, 1, 1)
        for timestamp in timestamps:
            plt.axvline(x=timestamp, color='#999999')
        n, bins, patches = plt.hist(lecture_submissions, weeks, histtype='bar', stacked=True, color=['#0d57aa','#ffcb0b'], label=['Views','Downloads'])
        plt.legend()
        plt.title('Lecture Submissions by Week ('+str(schema_name)+')')
        plt.xlabel('Date')
        plt.ylabel('Count')

        plt.subplot(2, 1, 2)
        for timestamp in timestamps:
            plt.axvline(x=timestamp, color='#999999')
        n, bins, patches = plt.hist(lecture_submissions, days, histtype='bar', stacked=True, color=['#0d57aa','#ffcb0b'], label=['Views','Downloads'])
        plt.legend()
        plt.title('Lecture Submissions by Day ('+str(schema_name)+')')
        plt.xlabel('Date')
        plt.ylabel('Count')
    except Exception, e:
        pass

    # Course grades
    try:
        query = """SELECT MIN(normal_grade) AS accomplishment_grade
                   FROM   course_grades
                   WHERE  achievement_level='normal'"""
        accomplishment_grade = pd.io.sql.read_frame(query, schemaconn.raw_connection()).ix[0,0]

        query = """SELECT MAX(temp.lectures_viewed) AS lecture_total
                   FROM   (
                          SELECT COUNT(DISTINCT(item_id)) AS lectures_viewed
                          FROM   lecture_submission_metadata
                          GROUP BY session_user_id
                          ) temp;"""
        lecture_total = pd.io.sql.read_frame(query, schemaconn.raw_connection()).ix[0,0]

        query = """SELECT course_grades.session_user_id
                        , course_grades.normal_grade
                        , lecture_submissions.lectures_viewed
                   FROM   course_grades
                          LEFT JOIN (
                               SELECT session_user_id
                                     , 100*COUNT(DISTINCT(item_id))/%s AS lectures_viewed
                               FROM   lecture_submission_metadata
                               GROUP BY session_user_id) lecture_submissions
                          ON course_grades.session_user_id=lecture_submissions.session_user_id
                   WHERE course_grades.normal_grade > 0;""" % lecture_total
        lectures_viewed = pd.io.sql.read_frame(query, schemaconn.raw_connection())

        grades_by_lectures_viewed = []
        bin_width = 10
        bin_start = 0
        bin_end = 0
        labels = []
        while bin_end < 100:
            bin_start = bin_end
            bin_end += bin_width
            if bin_end < 100:
                grades_by_lectures_viewed.append( lectures_viewed.loc[(lectures_viewed['lectures_viewed']>=bin_start) & (lectures_viewed['lectures_viewed']<bin_end),'normal_grade'].values )
                labels.append(str(bin_start)+'-'+str(bin_end)+'%')
            else:
                grades_by_lectures_viewed.append( lectures_viewed.loc[(lectures_viewed['lectures_viewed']>=bin_start) & (lectures_viewed['lectures_viewed']<=bin_end),'normal_grade'].values )
                labels.append(str(bin_start)+'-'+str(bin_end)+'%')

       # Plot course grade distribution, colored by percent of lectures viewed
        colors = []
        bin_num = 100/bin_width
        for i in range(bin_num):
            colors.append((float(i)/bin_num,0.2,1-float(i)/bin_num))
        plt.figure()
        n, bins, patches = plt.hist(grades_by_lectures_viewed, 100, histtype='bar', stacked=True, color=colors, label=labels)
        plt.axvline(x=accomplishment_grade, color='#999999')
        plt.legend(title='% of lectures accessed')
        plt.title('Distribution of Course Grades ('+str(schema_name)+')')
        plt.xlabel('Grade')
        plt.ylabel('Count')

        query = """SELECT session_user_id
                        , normal_grade
                        , achievement_level
                   FROM   course_grades
                   WHERE  normal_grade>0"""
        course_grades = pd.io.sql.read_frame(query, schemaconn.raw_connection())

        d,p = scipy.stats.kstest(course_grades['normal_grade'],'norm')
        mean = float(course_grades.mean())
        std = float(course_grades.std())
        print '  Course grades:'
        print '  mean='+str(mean)+', sd='+str(std)
        print '  Kolmogorov-Smirnov: D='+str(d)+', p='+str(p)
        print ''

        '''
        # Plot course grade distribution, colored by percent of lectures viewed
        colors = []
        labels = []
        time_len = len(grades_by_time)
        for i in range(time_len):
            colors.append((float(i)/time_len,0.2,1-float(i)/time_len))
            labels.append('Week '+str(i+1))
        labels[time_len-1] = 'Beyond'
        plt.figure()
        n, bins, patches = plt.hist(grades_by_time, 100, histtype='bar', stacked=True, color=colors, label=labels)
        plt.axvline(x=accomplishment_grade, color='#999999')
        plt.legend(title='Last lecture view')
        plt.title('Distribution of Course Grades')
        plt.xlabel('Grade')
        plt.ylabel('Count')
        '''
    except Exception, e:
        pass

    plt.show()