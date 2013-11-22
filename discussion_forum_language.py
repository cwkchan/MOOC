#    Copyright (C) 2013 The Regents of the University of Michigan
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

import enchant
import argparse
from enchant.checker import SpellChecker
from util.config import *
from multiprocessing import Pool

BATCH_SIZE=1000
"""Number of items to insert into the db at once"""

def process_single_language(schema_name, language):
	logger.info("Calculating spelling errors for schema {} using language {}".format(schema_name,language))
	
	#load spell checker for this language
	try:
		chkr = SpellChecker(language)
	except:
		logger.warn("Error using dictionary {}, skipping".format(language))
		return
	
	query="""select id, post_text from forum_posts"""
	results=get_connection(schema_name).execute(query)
	insert_values=[]
	for row in results:	
		logger.debug("Working on course {}, post {}, with a length of length {} using language {}".format(schema_name, row[0], len(row[1]),language))
		chkr.set_text(row[1])
		cnt=0
		for err in chkr:
			cnt+=1
		stats={"course_id":schema_name, "forum_posts_id":row[0], "language":language, "score":cnt}
		insert_values.append(stats)
		if len(insert_values)==BATCH_SIZE:
			batch_insert(conn, tbl_coursera_message_language, insert_values)
			insert_values=[]
	batch_insert(conn, tbl_coursera_message_language, insert_values)
	insert_values=[]
		
def batch_insert(connection, table, vals):
	"""Batch inserts a set of values into a given table
	"""
	if len(vals)>0:
		connection.execute( table.insert().values(vals) )
		logger.info("Inserted {} values into schema {}".format(len(vals), table.name))

parser = argparse.ArgumentParser(description='Create derivative statistics tables relating to discussion forums.')
parser.add_argument('--clean', action='store_true', help='Whether to drop tables in the database or not')
parser.add_argument('--schemas', help='An optional list of the schemas to create tables from', required=False)
parser.add_argument('--verbose', action='store_true', help='If this flag exists extended logging will be on')
parser.add_argument('--languages', action='store', help='A comma separated list of languages to try, if this flag is not provided all languages installed will be tried.')
parser.add_argument('--processes', action='store', default=1, help='Number of threads to run at once, this script is CPU bound  Default is 1.')
args = parser.parse_args()

logger=get_logger("discussion_forum_language.py",args.verbose)

conn=get_connection()
schemas=get_coursera_schema_list()

if (args.clean):
	query="DROP TABLE IF EXISTS coursera_message_language"
	try:
		conn.execute(query)
	except:
		pass

query="""CREATE TABLE IF NOT EXISTS `coursera_message_language` (
  `course_id` varchar(255) NOT NULL,
  `forum_posts_id` int(11) NOT NULL,
  `language` varchar(16) NOT NULL,
  `score` int(11) NOT NULL,
  PRIMARY KEY (`course_id`,`forum_posts_id`,`language`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;"""

conn.execute(query)
metadata = MetaData()
metadata.reflect(bind=conn)
tbl_coursera_message_language=metadata.tables["coursera_message_language"]

languages=[]
if (args.languages == None):
	languages=enchant.list_languages()
	logger.info("No languages specified, using all installed dictionaries")
else:
	languages=args.languages.split(",")
logger.info("Using the following languages: {}".format(languages))

p = Pool(int(args.processes))
for schema_name in schemas:
	for language in languages:
		p.apply_async(process_single_language,(schema_name,language))
p.close()
p.join()