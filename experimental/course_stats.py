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

def percent(list1, list2):
    percentages = []
    for i in range(0,len(list1)):
        percentages.append(int(100.0*list1[i]/list2[i]))
    return percentages

def rgb(r,g,b):
    return (r/255.0, g/255.0, b/255.0)

def session_histograms(array, title, bin_width):
    minimum = None
    for dist in array:
        if minimum==None or min(dist) < minimum:
            minimum = min(dist)
    maximum = None
    for dist in array:
        if maximum==None or max(dist) > maximum:
            maximum = max(dist)
    bins = np.ceil((maximum-minimum)/bin_width)
    plt.figure()
    for i in range(1,len(array)+1):
        plt.subplot(len(array), 1, i)
        n, bins, patches = plt.hist(array[i-1], bins=bins, range=[minimum, maximum])
        plt.ylabel('Count')
        if i==1:
            plt.title(title)

def timestamp_histograms(array, title, bin_width):
    aligned_timestamps = []
    for dist in array:
        minimum = min(dist)
        aligned_timestamps.append(map(lambda x: x-minimum, dist))
    maximum = None
    for dist in aligned_timestamps:
        if maximum==None or max(dist) > maximum:
            maximum = max(dist)
    bins = np.ceil((maximum)/bin_width)
    plt.figure()
    for i in range(1,len(aligned_timestamps)+1):
        plt.subplot(len(aligned_timestamps), 1, i)
        n, bins, patches = plt.hist(aligned_timestamps[i-1], bins=bins, range=[0, maximum])
        plt.ylabel('Count')
        if i==1:
            plt.title(title)

def pairwise_ks(array):
    print '  Pairwise Kolmogorov-Smirnov:'
    for i in range(0,len(array)):
        for j in range(i+1,len(array)):
            d,p = scipy.stats.ks_2samp(array[i],array[j])
            if p < .05:
                print '  00'+str(i+1)+'-00'+str(j+1)+': D='+str(d)+', p='+str(p)+' *'
            else:
                print '  00'+str(i+1)+'-00'+str(j+1)+': D='+str(d)+', p='+str(p)
    print ''

parser = argparse.ArgumentParser(description='Generate engagement for each user across course blocks.')
parser.add_argument('--courses', help='An optional list of courses to create tables from', required=False)
args = parser.parse_args()

conn = get_connection()

schemas = {}
if (args.courses != None):
    courses = args.courses.split(",")
    for course_id in courses:
        query = 'SELECT session_id FROM coursera_index WHERE session_id LIKE "'+course_id+'%";'
        schemas[course_id] = sorted(pd.io.sql.read_frame(query, conn.raw_connection())['session_id'].values)
else:
    # Get a list of the databases to run this query over
    query = """SELECT session_id from coursera_index;"""
    courses = list(set(map(lambda s: s[:-4], pd.io.sql.read_frame(query, conn.raw_connection())['session_id'].values)))
    for course_id in courses:
        query = 'SELECT session_id FROM coursera_index WHERE session_id LIKE "'+course_id+'%";'
        schemas[course_id] = sorted(pd.io.sql.read_frame(query, conn.raw_connection())['session_id'].values)

for course_id in courses:
    print "Working with "+course_id

    query = 'SELECT session_id,start,duration FROM coursera_index WHERE session_id LIKE "'+course_id+'%";'
    coursera_index = pd.io.sql.read_frame(query, conn.raw_connection(), index_col='session_id')

    course_grades = []
    lecture_views = []
    lecture_downloads = []

    sessions = []
    registered = []
    active = []
    graded = []
    statement_of_accomplishment = []

    for schema_name in schemas[course_id]:
        try:
            #print "  Working with schema " + str(schema_name)
            schemaconn = get_connection(schema_name)
        except Exception, e:
            # Chances are this database doesn't exist, move onto the next schema
            print '  Error accessing '+str(schema_name)+' with exception '+str(e)
            continue

        # Engagement numbers
        try:
            sessions.append(str(schema_name[-3:]))
            query = """SELECT COUNT(DISTINCT(session_user_id))
                       FROM   users;"""
            registered.append(pd.io.sql.read_frame(query, schemaconn.raw_connection()).ix[0,0])

            query = """SELECT COUNT(DISTINCT(session_user_id))
                       FROM   lecture_submission_metadata;"""
            active.append(pd.io.sql.read_frame(query, schemaconn.raw_connection()).ix[0,0])

            query = """SELECT COUNT(DISTINCT(session_user_id))
                       FROM   course_grades
                       WHERE  normal_grade>0;"""
            graded.append(pd.io.sql.read_frame(query, schemaconn.raw_connection()).ix[0,0])

            query = """SELECT COUNT(DISTINCT(session_user_id))
                       FROM   course_grades
                       WHERE  achievement_level='normal'
                          OR  achievement_level='distinction';"""
            statement_of_accomplishment.append(pd.io.sql.read_frame(query, schemaconn.raw_connection()).ix[0,0])
        except Exception, e:
            pass

        # Course grade distributions
        try:
            query = """SELECT normal_grade
                       FROM   course_grades
                       WHERE normal_grade>0;"""
            course_grades.append(pd.io.sql.read_frame(query, schemaconn.raw_connection())['normal_grade'].values)
        except Exception, e:
            pass

        # Lecture view distributions
        start_timestamp = int(time.mktime(time.strptime(coursera_index.loc[schema_name,'start'], '%m/%d/%Y'))) - time.timezone
        try:
            query = """SELECT submission_time
                       FROM   lecture_submission_metadata
                       WHERE  submission_time>=%s
                         AND  action='view';""" % str(start_timestamp)
            lecture_views.append(pd.io.sql.read_frame(query, schemaconn.raw_connection())['submission_time'].values)
        except:
            pass

        # Lecture download distributions
        start_timestamp = int(time.mktime(time.strptime(coursera_index.loc[schema_name,'start'], '%m/%d/%Y'))) - time.timezone
        try:
            query = """SELECT submission_time
                       FROM   lecture_submission_metadata
                       WHERE  submission_time>=%s
                         AND  action='download';""" % str(start_timestamp)
            lecture_downloads.append(pd.io.sql.read_frame(query, schemaconn.raw_connection())['submission_time'].values)
        except:
            pass

    print '  Sessions: '+str(sessions)
    print '  '+str(registered)+' registered users'
    print '  '+str(percent(active,registered))+'% of registered users were active (i.e., accessed a lecture) '+str(active)
    print '  '+str(percent(graded,registered))+'% of active users were graded '+str(graded)
    print '  '+str(percent(statement_of_accomplishment,registered))+'% of active users earned a Statement of Accomplishment '+str(statement_of_accomplishment)
    print ''

    plt.figure()
    plt.subplot(3, 1, 1)
    plt.plot(np.arange(len(registered)), registered, marker='o', color='#666666', label='Registered')
    plt.plot(np.arange(len(registered)), active, marker='o', color=rgb(97,156,255), label='Active')
    plt.plot(np.arange(len(registered)), graded, marker='o', color=rgb(0,186,56), label='Graded')
    plt.plot(np.arange(len(registered)), statement_of_accomplishment, marker='o', color=rgb(248,118,109), label='Statement of Accomplishment')
    plt.xticks( np.arange(len(registered)), sessions[:len(registered)] )
    plt.ylabel('Count')
    plt.legend()
    plt.title('User Statistics by Session ('+course_id+')')

    plt.subplot(3, 1, 2)
    plt.plot(np.arange(len(registered)), percent(active,registered), marker='o', color=rgb(97,156,255), label='% Active')
    plt.plot(np.arange(len(registered)), percent(graded,registered), marker='o', color=rgb(0,186,56), label='% Graded')
    plt.plot(np.arange(len(registered)), percent(statement_of_accomplishment,registered), marker='o', color=rgb(248,118,109), label='% Statement of Accomplishment')
    plt.xticks(np.arange(len(registered)), sessions[:len(registered)])
    plt.ylabel('Percentage of Registered Users')
    plt.ylim(0, 100)
    plt.legend()
    plt.title('Normalized by Number of Registered Users')

    plt.subplot(3, 1, 3)
    plt.plot(np.arange(len(registered)), percent(graded,active), marker='o', color=rgb(0,186,56), label='% Graded')
    plt.plot(np.arange(len(registered)), percent(statement_of_accomplishment,active), marker='o', color=rgb(248,118,109), label='% Statement of Accomplishment')
    plt.xlabel('Session')
    plt.xticks(np.arange(len(registered)), sessions[:len(registered)])
    plt.ylabel('Percentage of Active Users')
    plt.ylim(0, 100)
    plt.legend()
    plt.title('Normalized by Number of Active Users')

    print '  Course grade distributions:'
    pairwise_ks(course_grades)
    session_histograms(course_grades, 'Course Grades ('+course_id+')', 5)

    print '  Lecture view distributions:'
    pairwise_ks(lecture_views)
    timestamp_histograms(lecture_views, 'Lecture Views ('+course_id+')', 604800)

    print '  Lecture download distributions:'
    pairwise_ks(lecture_downloads)
    timestamp_histograms(lecture_downloads, 'Lecture Downloads ('+course_id+')', 604800)

    plt.show()