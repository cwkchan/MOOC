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

import argparse
from util.config import *
from readability_score.calculators.fleschkincaid import FleschKincaid
from StringIO import StringIO
from postings.posting_statistics import *

parser = argparse.ArgumentParser(description='Create derivative statistics tables relating to discussion forums.')
parser.add_argument('--clean', action='store_true', help='Whether to drop tables in the database or not')
parser.add_argument('--verbose', action='store_true', help='If this flag exists extended logging will be on')
args = parser.parse_args()

logger=get_logger("discussion_forum_readability.py",args.verbose)

conn=get_connection()
schemas=get_coursera_schema_list()

if (args.clean):
	query="DROP TABLE IF EXISTS coursera_message_stats"
	try:
		conn.execute(query)
	except:
		pass

query="""CREATE TABLE IF NOT EXISTS `coursera_message_stats` (
  `course_id` varchar(128) NOT NULL,
  `correct_post_text` MEDIUMTEXT DEFAULT NULL,
  `emails` text DEFAULT NULL,
  `fk_readability` float DEFAULT NULL,
  `forum_posts_id` int(11) NOT NULL,
  `letter_count` int(11) DEFAULT NULL,
  `min_age` float DEFAULT NULL,
  `misspellings` int(11) DEFAULT NULL,
  `polysyllword_count` int(11) DEFAULT NULL,
  `sent_count` int(11) DEFAULT NULL,
  `sentlen_average` float DEFAULT NULL,
  `simpleword_count` int(11) DEFAULT NULL,
  `syll_count` int(11) DEFAULT NULL,
  `us_grade` float DEFAULT NULL,
  `urls` text DEFAULT NULL,
  `word_count` int(11) DEFAULT NULL,
  `wordlen_average` float DEFAULT NULL,
  `wordletter_average` float DEFAULT NULL,
  `wordsent_average` float DEFAULT NULL,
  PRIMARY KEY (`course_id`,`forum_posts_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf32;"""

conn.execute(query)
metadata = MetaData()
metadata.reflect(bind=conn)
tbl_coursera_message_stats=metadata.tables["coursera_message_stats"]

for schema_name in schemas:
	schema_conn=get_connection(schema_name)
	results=schema_conn.execute("select id, post_text from forum_posts")
	logger.indo("Working with course {}".format(stats["course_id"]))
	
	for row in results:
	  stats= generate_post_statistics(row["post_text"])
	  stats["forum_posts_id"]=row["id"]
	  stats["course_id"]=schema_name
	  conn.execute( tbl_coursera_message_stats.insert().values(stats) )
	  logger.debug(schema_name + str(stats["forum_posts_id"]))