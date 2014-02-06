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

    # Extract question_text and add to question_index
    question_index = []
    header = df[:1]
    df = df.drop(df.index[:1])
    for i, field in enumerate(header.ix[0]):
      question = str(header.columns[i]) #.replace('\xEF\xBB\xBFV1', 'V1')
      question_text = str(field)
      if question.find('Unnamed: ') == -1:
        question_index.append({'session_id':session_id, 'question':question, 'question_text':question_text})
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

    try:
      query = 'CREATE TABLE IF NOT EXISTS `'+session_id+'` ('
      varchar_list = ['V1','V2','V3','V4','V5','V6','V7','V8','V9','V10','user_id','demo']
      for q in question_index:
        if any(q['question'] in f for f in varchar_list):
          query += '`'+q['question']+'` VARCHAR(255) DEFAULT NULL, '
        else:
          query += '`'+q['question']+'` MEDIUMTEXT CHARACTER SET utf32 DEFAULT NULL, '
      query += 'PRIMARY KEY (V1)'
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

    '''
    # Write survey_responses to table
    if (args.clean):
      query = """DROP TABLE IF EXISTS `%s`;""" % session_id
      try:
        conn.execute(query)
      except:
        pass

    try:
      query = """
        CREATE TABLE IF NOT EXISTS `%s` (
          `id` INT NOT NULL AUTO_INCREMENT, """ % session_id
      for row in question_index:
        query += """`%s` VARCHAR(255) DEFAULT NULL, """ % row['question']
      query += """
          PRIMARY KEY (id)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
        """
      conn.execute(query)
    except:
      pass

    #pd.io.sql.write_frame(df, '`'+session_id+'`', conn.raw_connection(), flavor='mysql', if_exists='append')
    '''
