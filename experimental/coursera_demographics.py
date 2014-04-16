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
from os import listdir
from util.config import *

parser = argparse.ArgumentParser(description='Copy sections_blocks tables from csv to SQL database.')
parser.add_argument('--clean', action='store_true', help='Whether to drop tables in the database or not')
parser.add_argument('--verbose', action='store_true', help='Whether to debug log or not')
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--dir', help='A directory with CSV files in it')
args = parser.parse_args()

conn = get_connection()

if (args.clean):
  try:
    query = """DROP TABLE IF EXISTS `coursera_demographics_question_index`;"""
    conn.execute(query)
  except:
    pass
  
  try:
    query = """DROP TABLE IF EXISTS `coursera_demographics`;"""
    conn.execute(query)
  except:
    pass

try:
  query = """
    CREATE TABLE IF NOT EXISTS `coursera_demographics_question_index` (
      question VARCHAR(255) NOT NULL,
      question_text MEDIUMTEXT DEFAULT NULL,
      PRIMARY KEY (question)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
    """
  conn.execute(query)
except:
  pass

# Check which sessions are already in the database
try:
  query = """SELECT DISTINCT `session_id` FROM `coursera_demographics`;"""
  existing = []
  for row in conn.execute(query):
    existing.append(str(row['session_id']))
except:
  pass

for csv in listdir(args.dir):
  session_id = filename_to_schema(csv)
  if session_id not in existing:
    df = pd.io.parsers.read_csv(args.dir+'/'+csv)
    df = df.rename(columns = {'\xEF\xBB\xBFV1':'V1'})
    df = df.drop_duplicates(cols='V3', take_last=True)

    # Extract question_text and add to question_index
    question_index = []
    comments_index = 'Q16'
    header = df[:1]
    header.ix[0]
    df = df.drop(df.index[:1])
    for i, field in enumerate(header.ix[0]):
      question = str(header.columns[i])
      question_text = str(field)
      if question.find('Unnamed: ') == -1:
        question_index.append({'question':question, 'question_text':question_text})
      else:
        del df[question]
    question_index_df = pd.DataFrame(question_index)

    try:
      pd.io.sql.write_frame(question_index_df, 'coursera_demographics_question_index', conn.raw_connection(), flavor='mysql', if_exists='append')
    except:
      pass

    # Write survey_responses to table
    try:
      query = 'CREATE TABLE IF NOT EXISTS `coursera_demographics` ('
      query += '`session_id` VARCHAR(255) NOT NULL, '
      for q in question_index:
        if q['question']==comments_index:
          query += '`'+q['question']+'` MEDIUMTEXT CHARACTER SET utf32 DEFAULT NULL, '
        else:
          query += '`'+q['question']+'` VARCHAR(255) DEFAULT NULL, '
      query += 'PRIMARY KEY (`session_id`,`V3`)'
      query += ') ENGINE=InnoDB DEFAULT CHARSET=latin1;'

      conn.execute(query)
    except:
      pass
    
    try:
      query = 'INSERT INTO `coursera_demographics` (`session_id`, '
      for q in question_index:
        query += '`'+q['question']+'`,'
      query = query[:-1]+') VALUES '
      for index, row in df.iterrows():
        query += '("'+session_id+'",'
        for q in question_index:
          try:
            response_str = str(row[q['question']])
            response_str = response_str.replace('"',"\'")
            response_str = response_str.replace('%',' percent')
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
