# -*- coding: utf-8 -*-

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
import re
from os import listdir
from util.config import *

parser = argparse.ArgumentParser(description='Copy Qualtrics survey responses from csv to SQL database.')
parser.add_argument('--clean', action='store_true', help='Whether to drop tables in the database or not')
parser.add_argument('--verbose', action='store_true', help='Whether to debug log or not')
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--dir', help='A directory with CSV files in it')
args = parser.parse_args()

logger = get_logger("coursera_qualtrics_surveys.py",args.verbose)
conn = get_connection("qualtrics_surveys")

if (args.clean):
  try:
    query = """DROP TABLE IF EXISTS question_index;"""
    conn.execute(query)
  except:
    pass

try:
  query = """
    CREATE TABLE IF NOT EXISTS question_index (
      session_id VARCHAR(255) NOT NULL,
      question VARCHAR(255) NOT NULL,
      question_text VARCHAR(255) DEFAULT NULL,
      PRIMARY KEY (session_id, question)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
    """
  conn.execute(query)
except:
  pass

if (args.clean):
  try:
    query = """DROP TABLE IF EXISTS question_summary;"""
    conn.execute(query)
  except:
    pass

try:
  query = """
    CREATE TABLE IF NOT EXISTS question_summary (
      session_id VARCHAR(255) NOT NULL,
      user_id VARCHAR(255) NOT NULL,
      label VARCHAR(255) NOT NULL,
      response MEDIUMTEXT CHARACTER SET utf32 DEFAULT NULL,
      PRIMARY KEY (session_id, user_id, label)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
    """
  conn.execute(query)
except:
  pass

#question_summary = ['Q1','Q2','Q3','Q4','achieved_goals','materials_syllabus','materials_lecturevideos','materials_invideoquizzes','materials_assessment','materials_forums','work_on_own','work_online_knew','work_online_met','work_inperson_knew','work_inperson_met','workload','difficulty','pacing','time_management','certificate_motivation','perform_better_academic','perform_better_work','pursue_topic','problem_solving','confidence_learning','mention_employers','mention_education']
question_summary = ['achieved_goals','certificate_motivation','comments_TEXT','comm_blogs','comm_email','comm_fellow_students','comm_inperson','comm_other','comm_other_TEXT','comm_socialnetwork','comm_textmsg','comm_why_TEXT','compare_facetoface_TEXT','compare_online_TEXT','confidence_learning','difficulty','EmailAddress','EndDate','ExternalDataReference','Finished','hours_per_week','instructor_connection','IPAddress','like_best_TEXT','like_least_TEXT','LocationAccuracy','LocationLatitude','LocationLongitude','materials_assessment','materials_forums','materials_invideoquizzes','materials_lecturevideos','materials_syllabus','mention_education','mention_employers','meta_browser','meta_flash','meta_java','meta_os','meta_screenres','meta_useragent','meta_version','Name','pacing','perform_better_academic','perform_better_work','problem_solving','pursue_topic','Q1','Q2','Q3','Q4','recommend_to_friend','resources_forums','resources_lectureslides','resources_lecturevideos','resources_other','resources_other_TEXT','resources_peersnotes','resources_personalnotes','resources_recommendedtexts','ResponseID','ResponseSet','revisit_materials','StartDate','Status','stopped_participating','stopped_participating_TEXT','take_again_course','take_again_institution','take_again_instructor','take_again_um','time_management','user_id','welcome','workload','work_inperson_knew','work_inperson_met','work_online_knew','work_online_met','work_on_own']

# Remove Qualtrics metadata fields
skip = ['question','ResponseID','ResponseSet','Name','ExternalDataReference','EmailAddress','Status','StartDate','EndDate','Finished','meta_browser','meta_version','meta_os','meta_screenres','meta_flash','meta_java','meta_useragent','welcome']
for question in skip:
    if question in question_summary:
        question_summary.remove(question)
# Remove text fields
for question in question_summary:
    if question.find('_TEXT') != -1:
        question_summary.remove(question)

# Attempt to get coursera_qualtrics_map definitions
try:
  df = pd.io.parsers.read_csv('coursera_qualtrics_map.csv')
  coursera_qualtrics_map = {}
  for index, row in df.iterrows():
    coursera_qualtrics_map[row[0].replace('\x92','').replace('\xa0','')] = row[1]
except:
  pass

# Check which sessions are already in the database
query = """SELECT DISTINCT session_id FROM question_index;"""
existing = []
for row in conn.execute(query):
  existing.append(str(row['session_id']))

for csv in listdir(args.dir):
  session_id = filename_to_schema(csv)
  print session_id
  if session_id not in existing:
    df = pd.io.parsers.read_csv(args.dir+'/'+csv)
    df = df.rename(columns = {'\xEF\xBB\xBFV1':'V1'})

    # Drop empty field
    try:
      del df['Enter Embedded Data Field Name Here...']
    except:
      pass

    # Rename fields based on coursera_qualtrics_map definitions
    try:
      for i, field in enumerate(df.ix[0]):
        question = str(field)
        question_clean = re.sub(r'[^a-zA-Z\<\>\s][^a-zA-Z\<\>\s] / ',' / ',question).replace('’','')
        if question_clean in coursera_qualtrics_map:
          df = df.rename(columns = {df.columns[i]:coursera_qualtrics_map[question_clean]})
    except:
      pass

    # Extract question_text and add to question_index
    question_index = []
    question_map = {}
    header = df[:1]
    df = df.drop(df.index[:1])
    for i, field in enumerate(header.ix[0]):
      question = str(header.columns[i])
      question_text = str(field)
      if question.find('Unnamed: ') == -1:
        question_index.append({'session_id':session_id, 'question':question, 'question_text':question_text})
        question_map[question] = question_text
      else:
        del df[question]
    question_index_df = pd.DataFrame(question_index)
    pd.io.sql.write_frame(question_index_df, 'question_index', conn.raw_connection(), flavor='mysql', if_exists='append')

    # Write survey_responses to table
    if (args.clean):
      try:
        query = """DROP TABLE IF EXISTS `%s`;""" % session_id
        conn.execute(query)
      except:
        pass

    # Remove duplicate user responses
    df = df.drop_duplicates(cols='user_id', take_last=True)

    try:
      query = 'CREATE TABLE IF NOT EXISTS `'+session_id+'` ('
      varchar_list = ['ResponseID','ResponseSet','Name','ExternalDataReference','EmailAddress','IPAddress','Status','StartDate','EndDate','Finished','user_id','demo']
      for q in question_index:
        if any(q['question'] in f for f in varchar_list):
          query += '`'+q['question']+'` VARCHAR(255) DEFAULT NULL, '
        else:
          query += '`'+q['question']+'` MEDIUMTEXT CHARACTER SET utf32 DEFAULT NULL, '
      query += 'PRIMARY KEY (`ResponseID`)'
      query += ') ENGINE=InnoDB DEFAULT CHARSET=latin1;'

      conn.execute(query)
    except:
      pass

    try:
      query = """INSERT INTO `%s` (""" % session_id
      for q in question_index:
        query += '`'+q['question']+'`,'
      query = query[:-1]+') VALUES '
      for index, row in df.iterrows():
        query += '('
        for q in question_index:
          try:
            response_str = str(row[q['question']])
            response_str = response_str.replace('"',"\'")
            response_str = response_str.replace('%',' percent')
            response_str = response_str.replace('\xF0\x9F\x98\x8A','')
            if response_str=='nan':
              query += 'NULL,'
            else:
              query += '"'+response_str+'",'
          except:
            query += 'NULL,'
        query = query[:-1]+'),'
      query = query[:-1]

      conn.execute(query)
    except Exception, e:
      print e

    try:
      query = 'INSERT INTO `question_summary` (`session_id`,`user_id`,`label`,`response`) VALUES '
      #query = 'INSERT INTO `question_summary` (`session_id`,`user_id`,`label`,`question`,`response`) VALUES '
      for index, row in df.iterrows():
        for label in question_summary:
          try:
            response_str = str(row[label])
            response_str = response_str.replace('"',"\'")
            response_str = response_str.replace('%',' percent')
            response_str = response_str.replace('\xF0\x9F\x98\x8A','')
            if response_str=='nan':
              query += '("'+session_id+'","'+str(row['user_id']).replace('%','')+'","'+label+'",NULL),'
              #query += '("'+session_id+'","'+str(row['user_id']).replace('%','')+'","'+label+'","'+question_map[label]+'",NULL),'
            else:
              query += '("'+session_id+'","'+str(row['user_id']).replace('%','')+'","'+label+'","'+response_str+'"),'
              #query += '("'+session_id+'","'+str(row['user_id']).replace('%','')+'","'+label+'","'+question_map[label]+'","'+response_str+'"),'
          except Exception, e:
            print e
      query = query[:-1]

      conn.execute(query)
    except Exception, e:
      print e