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
from util.config import *

def checkTableExists(dbcon, tablename):
  dbcur = dbcon.cursor()
  dbcur.execute("""
    SELECT COUNT(*)
    FROM information_schema.tables
    WHERE table_name = '{0}'
    """.format(tablename.replace('\'', '\'\'')))
  if dbcur.fetchone()[0] == 1:
    dbcur.close()
    return True

  dbcur.close()
  return False

def audit_level(row):
  if (row['hard_close_time'] - row['submission_time']) >= 0:
    return 2
  else:
    return 1

def block_col(row, i):
  if int(row['block_id']) == i:
    return row['engagement']
  else:
    return 0

parser = argparse.ArgumentParser(description='Generate engagement for each user across course blocks.')
parser.add_argument('--clean', action='store_true', help='Whether to drop tables in the database or not')
parser.add_argument('--schemas', help='An optional list of the schemas to create tables from', required=False)
args = parser.parse_args()

conn = get_connection()

if (args.clean):
  query = """DROP TABLE IF EXISTS coursera_engagement_umich;"""
  try:
    conn.execute(query)
  except:
    pass

try:
  query = """
    CREATE TABLE IF NOT EXISTS `coursera_engagement_umich` (
    `session_id` varchar(255) NOT NULL,
    `user_id` int(11) NOT NULL,
    `achievement_level` enum('normal','distinction','none') NOT NULL,
    `block_0` tinyint DEFAULT NULL,
    `block_1` tinyint DEFAULT NULL,
    `block_2` tinyint DEFAULT NULL,
    `block_3` tinyint DEFAULT NULL,
    `block_4` tinyint DEFAULT NULL,
    `block_5` tinyint DEFAULT NULL,
    `block_6` tinyint DEFAULT NULL,
    `block_7` tinyint DEFAULT NULL,
    `block_8` tinyint DEFAULT NULL,
    `block_9` tinyint DEFAULT NULL,
    `block_10` tinyint DEFAULT NULL,
    PRIMARY KEY (`session_id`, `user_id`)
  ) ENGINE=InnoDB DEFAULT CHARSET=latin1;"""
  conn.execute(query)
except:
  pass

if (args.schemas != None):
  schemas = args.schemas.split(",");
else:
  # Get a list of the databases to run this query over
  query = """SELECT id from coursera_index;"""
  schemas = []
  for row in conn.execute(query):
    schemas.append(row[0].encode('ascii','ignore'))

  # Check which sessions are already in the database
  query = """SELECT DISTINCT session_id FROM coursera_engagement_umich;"""
  existing = []
  for row in conn.execute(query):
    existing.append(row.session_id.encode('ascii','ignore'))

for schema_name in schemas:
  if schema_name not in existing:
    try:
      print "Working with schema " + str(schema_name)
      schemaconn = get_connection(schema_name)
    except Exception, e:
      # Chances are this database doesn't exist, move onto the next schema
      print 'Error accessing '+str(schema_name)+' with exception '+str(e)
      continue

    # Check if block mappings are defined
    query = """SELECT section_id, block_id FROM coursera_map_sections_blocks WHERE session_id='%s'""" % schema_name
    sections_blocks = pd.io.sql.read_frame(query, conn.raw_connection(), index_col='section_id')
    if sections_blocks.empty:
      print 'Error: sections_blocks for '+schema_name+' not found!'
      continue

    # Get list of items with associated section_ids and hard_close_times
    query = """
      SELECT items_sections.item_type
           , items_sections.item_id
           , items_sections.section_id
           , COALESCE(lecture_metadata.hard_close_time, quiz_metadata.hard_close_time, assignment_metadata.hard_close_time) AS hard_close_time
      FROM   items_sections
             LEFT JOIN lecture_metadata
               ON items_sections.item_id = lecture_metadata.id
               AND items_sections.item_type = 'lecture'
             LEFT JOIN quiz_metadata
               ON items_sections.item_id = quiz_metadata.id
               AND items_sections.item_type = 'quiz'
             LEFT JOIN assignment_metadata
               ON items_sections.item_id = assignment_metadata.id
               AND items_sections.item_type = 'assignment'
      WHERE  lecture_metadata.deleted = 0
         OR  (quiz_metadata.deleted = 0 AND (quiz_metadata.quiz_type = 'quiz' OR quiz_metadata.quiz_type = 'homework' OR quiz_metadata.quiz_type = 'exam'))
         OR  assignment_metadata.deleted = 0;
      """
    df_items = pd.io.sql.read_frame(query, schemaconn.raw_connection(), index_col=['item_type', 'item_id'])

    # Check if course contains hg_assessments
    query = """
      SELECT id AS item_id
           , grading_deadline + grading_deadline_grace_period AS hard_close_time
      FROM   hg_assessment_metadata
      WHERE  deleted = 0;
      """
    df_hg_assessments = pd.io.sql.read_frame(query, schemaconn.raw_connection())

    # Check if section mappings are defined, then add to list of items
    if not df_hg_assessments.empty:
      query = """SELECT item_type, item_id, section_id FROM coursera_map_hg_assessment_sections WHERE session_id='%s'""" % schema_name
      hg_assessment_sections = pd.io.sql.read_frame(query, conn.raw_connection(), index_col='item_id')
      if hg_assessment_sections.empty:
        print 'Error: coursera_map_hg_assessment_sections for '+schema_name+' must be defined!'
        break

      df_hg_assessments = df_hg_assessments.join(hg_assessment_sections, on='item_id')
      df_hg_assessments = df_hg_assessments.join(sections_blocks, on='section_id')
      df_hg_assessments = df_hg_assessments[['item_type', 'item_id', 'section_id', 'hard_close_time']]
      df_hg_assessments.set_index(['item_type', 'item_id'], inplace=True)
      df_items = pd.concat([df_items, df_hg_assessments])

    # Get latest hard_close_time for each block
    df_items = df_items.join(sections_blocks, on='section_id')
    block_hard_close_time = df_items.groupby('block_id', as_index=False).max()['hard_close_time']

    # Initialize dataframe of users and set default engagement for each block (out = 0)
    try:
      query = """
        SELECT hash_mapping.user_id
             , hash_mapping.anon_user_id
             , course_grades.achievement_level
        FROM   hash_mapping
               INNER JOIN course_grades
                 ON hash_mapping.anon_user_id = course_grades.anon_user_id;
        """
      df_engagement = pd.io.sql.read_frame(query, schemaconn.raw_connection(), index_col='anon_user_id')
    except Exception, e:
      query = """
        SELECT hash_mapping.user_id
             , hash_mapping.session_user_id
             , course_grades.achievement_level
        FROM   hash_mapping
               INNER JOIN course_grades
                 ON hash_mapping.session_user_id = course_grades.anon_user_id;
        """
      df_engagement = pd.io.sql.read_frame(query, schemaconn.raw_connection(), index_col='session_user_id')

    block_min = int(sections_blocks.min())
    block_max = int(sections_blocks.max())
    for i in range(block_min, block_max+1):
      block_label = 'block_'+str(i)
      df_engagement[block_label] = 0

    # Iterate through lecture submissions and update users' engagement accordingly
    # (auditing concurrent = 1, auditing delayed = 2)
    query = """
      SELECT lecture_submission_metadata.anon_user_id
           , lecture_submission_metadata.submission_time
           , items_sections.section_id
      FROM   lecture_submission_metadata
             INNER JOIN lecture_metadata
               ON lecture_submission_metadata.item_id = lecture_metadata.id
             INNER JOIN items_sections
               ON lecture_submission_metadata.item_id = items_sections.item_id
               AND items_sections.item_type = 'lecture'
      WHERE  lecture_metadata.deleted = 0;
      """
    for row in schemaconn.execute(query):
      try:
        block_label = 'block_'+str(int(sections_blocks.loc[row.section_id]))
        if row.submission_time <= block_hard_close_time.loc[int(sections_blocks.loc[row.section_id])]:
          df_engagement.loc[row.anon_user_id, block_label] = 2
        elif df_engagement.loc[row.anon_user_id, block_label] < 1:
          df_engagement.loc[row.anon_user_id, block_label] = 1
      except Exception, e:
        pass
    
    # Iterate through quiz, assignment, and hg_assessment submissions and update users' engagement accordingly
    # (engaged = 4)
    query = """
      SELECT DISTINCT quiz_submission_metadata.anon_user_id
           , items_sections.section_id
      FROM   quiz_submission_metadata
             INNER JOIN quiz_metadata
               ON quiz_submission_metadata.item_id = quiz_metadata.id
             INNER JOIN items_sections
               ON quiz_submission_metadata.item_id = items_sections.item_id
               AND items_sections.item_type = 'quiz'
      WHERE  quiz_metadata.deleted = 0
        AND  (quiz_metadata.quiz_type = 'quiz' OR quiz_metadata.quiz_type = 'homework' OR quiz_metadata.quiz_type = 'exam');
      """
    for row in schemaconn.execute(query):
      try:
        block_label = 'block_'+str(int(sections_blocks.loc[row.section_id]))
        if df_engagement.loc[row.anon_user_id, block_label] < 4:
          df_engagement.loc[row.anon_user_id, block_label] = 4
      except Exception, e:
        pass

    query = """
      SELECT DISTINCT assignment_submission_metadata.anon_user_id
           , items_sections.section_id
      FROM   assignment_submission_metadata
             INNER JOIN assignment_part_metadata
               ON assignment_submission_metadata.item_id = assignment_part_metadata.id
             INNER JOIN assignment_metadata
               ON assignment_part_metadata.assignment_id = assignment_metadata.id
             INNER JOIN items_sections
               ON assignment_metadata.id = items_sections.item_id
               AND items_sections.item_type = 'assignment'
      WHERE  assignment_part_metadata.deleted = 0
        AND  assignment_metadata.deleted = 0;
      """
    for row in schemaconn.execute(query):
      try:
        block_label = 'block_'+str(int(sections_blocks.loc[row.section_id]))
        if df_engagement.loc[row.anon_user_id, block_label] < 4:
          df_engagement.loc[row.anon_user_id, block_label] = 4
      except Exception, e:
        pass

    if not df_hg_assessments.empty:
      if not hg_assessment_sections.empty:
        query = """
          SELECT DISTINCT hg_assessment_submission_metadata.anon_user_id
               , hg_assessment_submission_metadata.assessment_id
          FROM   hg_assessment_submission_metadata
                 INNER JOIN hg_assessment_metadata
                   ON hg_assessment_submission_metadata.assessment_id = hg_assessment_metadata.id
          WHERE  hg_assessment_metadata.deleted = 0;
          """
      for row in schemaconn.execute(query):
        try:
          section_id = hg_assessment_sections.loc[row.assessment_id]
          block_label = 'block_'+str(int(sections_blocks.loc[section_id]))
          if df_engagement.loc[row.anon_user_id, block_label] < 4:
            df_engagement.loc[row.anon_user_id, block_label] = 4
        except Exception, e:
          pass

    for i in range(block_max+1, 10+1):
      block_label = 'block_'+str(i)
      df_engagement[block_label] = None
    df_engagement['session_id'] = schema_name
    df_engagement = df_engagement[['session_id', 'user_id', 'achievement_level', 'block_1', 'block_2', 'block_3', 'block_4', 'block_5', 'block_6', 'block_7', 'block_8', 'block_9', 'block_10']]
    pd.io.sql.write_frame(df_engagement, 'coursera_engagement_umich', conn.raw_connection(), flavor='mysql', if_exists='append')
