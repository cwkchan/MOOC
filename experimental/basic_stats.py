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

def generate_plot_percentage_range(bins):
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
        labels = []
        for bin in bins:
            grades_by_lectures_viewed.append( lectures_viewed.loc[(lectures_viewed['lectures_viewed']>bin[0]) & (lectures_viewed['lectures_viewed']<=bin[1]),'normal_grade'].values )
            labels.append(str(bin[0])+'-'+str(bin[1])+'%')

       # Plot course grade distribution, colored by percent of lectures viewed
        colors = []
        bin_num = len(bins)
        for i in range(bin_num):
            colors.append((float(i)/bin_num,0.2,1-float(i)/bin_num))
        n, bins, patches = plt.hist(grades_by_lectures_viewed, 100, histtype='bar', stacked=True, color=colors, label=labels)
        plt.axvline(x=accomplishment_grade, color='#999999')
        plt.legend(title='% of lectures accessed')
        plt.title('Distribution of Course Grades ('+str(schema_name)+')')
        plt.xlabel('Grade')
        plt.ylabel('Count')

    except Exception, e:
        print e

def generate_plot_week_range(bins):
    try:
        query = 'SELECT session_id,start,duration FROM coursera_index WHERE session_id="%s"' % schema_name
        coursera_index = pd.io.sql.read_frame(query, conn.raw_connection(), index_col='session_id')
        start_timestamp = int(time.mktime(time.strptime(coursera_index.loc[schema_name,'start'], '%m/%d/%Y'))) - time.timezone
        duration = coursera_index.loc[schema_name,'duration']
        timestamp_bins = []
        labels = []
        for bin in bins:
            if bin[1]!=None and bin[1]<=duration:
                timestamp_bins.append(((bin[0]-1)*604800+start_timestamp,bin[1]*604800+start_timestamp))
                if bin[0]==bin[1]:
                    labels.append('Week '+str(bin[1]))
                else:
                    labels.append('Week '+str(bin[0])+'-'+str(bin[1]))
            else:
                timestamp_bins.append(((bin[0]-1)*604800+start_timestamp,None))
                if bin[0]<duration:
                    labels.append('Week '+str(bin[0])+'-Beyond')
                else:
                    labels.append('Beyond')
                break

        query = """SELECT MIN(normal_grade) AS accomplishment_grade
                   FROM   course_grades
                   WHERE  achievement_level='normal'"""
        accomplishment_grade = pd.io.sql.read_frame(query, schemaconn.raw_connection()).ix[0,0]

        query = """SELECT course_grades.session_user_id
                        , normal_grade
                        , last_submission_time
                   FROM   course_grades
                          LEFT JOIN (
                               SELECT session_user_id
                                    , MAX(submission_time) AS last_submission_time
                               FROM   lecture_submission_metadata
                               GROUP BY session_user_id) lecture_submissions
                          ON course_grades.session_user_id=lecture_submissions.session_user_id
                   WHERE normal_grade>0;"""
        last_lecture_views = pd.io.sql.read_frame(query, schemaconn.raw_connection())

        grades_by_last_lecture_view = []
        for bin in timestamp_bins:
            if bin[1]!=None:
                grades_by_last_lecture_view.append( last_lecture_views.loc[(last_lecture_views['last_submission_time']>bin[0]) & (last_lecture_views['last_submission_time']<=bin[1]),'normal_grade'].values )
            else:
                grades_by_last_lecture_view.append( last_lecture_views.loc[(last_lecture_views['last_submission_time']>bin[0]),'normal_grade'].values )

       # Plot course grade distribution, colored by last lecture view
        colors = []
        bin_num = len(timestamp_bins)
        for i in range(bin_num):
            colors.append((float(i)/bin_num,0.2,1-float(i)/bin_num))
        n, bins, patches = plt.hist(grades_by_last_lecture_view, 100, histtype='bar', stacked=True, color=colors, label=labels)
        plt.axvline(x=accomplishment_grade, color='#999999')
        plt.legend(title='Last lecture view')
        plt.title('Distribution of Course Grades ('+str(schema_name)+')')
        plt.xlabel('Grade')
        plt.ylabel('Count')

    except Exception, e:
        print e

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
        print e

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
    plt.figure()
    plt.subplot(2, 1, 1)
    generate_plot_percentage_range([(0,10),(10,20),(20,30),(30,40),(40,50),(50,60),(60,70),(70,80),(80,90),(90,100)])
    plt.subplot(2, 1, 2)
    generate_plot_week_range([(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10),(11,11),(12,12),(13,13),(14,14),(15,15),(16,16),(17,17),(18,18),(19,19),(20,20)])

    try:
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
    except Exception, e:
        print e

    plt.show()