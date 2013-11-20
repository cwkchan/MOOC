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

parser = argparse.ArgumentParser(description='Create derivative statistics tables relating to discussion forums.')
parser.add_argument('--clean', action='store_true', help='Whether to drop tables in the database or not')
args = parser.parse_args()

conn=get_connection()
schemas=get_coursera_schema_list();

if (args.clean):
	query="DROP TABLE IF EXISTS coursera_message_stats"
	conn.execute(query)

query="""CREATE TABLE IF NOT EXISTS `coursera_message_stats` (
  `letter_count` int(11) DEFAULT NULL,
  `polysyllword_count` int(11) DEFAULT NULL,
  `sent_count` int(11) DEFAULT NULL,
  `sentlen_average` float DEFAULT NULL,
  `simpleword_count` int(11) DEFAULT NULL,
  `syll_count` int(11) DEFAULT NULL,
  `word_count` int(11) DEFAULT NULL,
  `wordlen_average` float DEFAULT NULL,
  `wordletter_average` float DEFAULT NULL,
  `wordsent_average` float DEFAULT NULL,
  `min_age` float DEFAULT NULL,
  `us_grade` float DEFAULT NULL,
  `course_id` varchar(255) NOT NULL,
  `forum_posts_id` int(11) NOT NULL,
  PRIMARY KEY (`course_id`,`forum_posts_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;"""

conn.execute(query)

for schema_name in schemas:
	
	sql_insert_coursera_message_stats="""INSERT INTO coursera_message_stats (letter_count,polysyllword_count,sent_count,sentlen_average,simpleword_count,syll_count,word_count,wordlen_average,wordletter_average,wordsent_average,min_age,us_grade,course_id,forum_posts_id) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
	
	try:
		print "Working with schema " + str(schema_name)
		conn=get_connection(schema_name)
	except Exception, e:
		#chances are this database just doesn't exist, just move onto the next schema
		print "Error accessing " + str(schema_name) + " with exception " + str(e)
		continue
	
	cursor=conn.cursor()
	cursor.execute("select id, post_text from forum_posts")
	
	for row in cursor:
	  fk = FleschKincaid(StringIO(row[1].encode("utf_8")).read(), locale='/usr/share/myspell/dicts/hyph_en_US.dic')
	  values=(fk.scores["letter_count"], 
	          fk.scores["polysyllword_count"],
	          fk.scores["sent_count"],
	          fk.scores["sentlen_average"],
	          fk.scores["simpleword_count"],
	          fk.scores["syll_count"],
	          fk.scores["word_count"],
	          fk.scores["wordlen_average"],
	          fk.scores["wordletter_average"],
	          fk.scores["wordsent_average"],
	          fk.min_age,
	          fk.us_grade,
	          schema_name,
	          row[0])
	  uselabcursor.execute(sql_insert_coursera_message_stats,values)
	  uselabconn.commit()

conn.close()
uselabconn.close()
