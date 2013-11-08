import enchant
import argparse
from sqlconnection import *
from enchant.checker import SpellChecker

parser = argparse.ArgumentParser(description='Create derivative statistics tables relating to discussion forums.')
parser.add_argument('--clean', action='store_true', help='Whether to drop tables in the database or not')
parser.add_argument('--schemas', help='An optional list of the schemas to create tables from', required=False)
args = parser.parse_args()

uselabconn=get_connection("uselab_mooc")
uselabcursor=uselabconn.cursor()

if (args.clean):
	sql_drop_coursera_message_stats="DROP TABLE IF EXISTS coursera_message_language"
	uselabcursor.execute(sql_drop_coursera_message_stats)
if (args.schemas != None):
	schemas=args.schemas.split(",");
else:
	#get a list of the databases to run this query over
	sql_select_databases="select id from coursera_index;"
	uselabcursor.execute(sql_select_databases);
	schemas=[]
	for row in uselabcursor:
		schemas.append(row[0].encode('ascii','ignore'))	
	
#create database
sql_create_coursera_message_language="""CREATE TABLE IF NOT EXISTS `coursera_message_language` (
  `course_id` varchar(255) NOT NULL,
  `forum_posts_id` int(11) NOT NULL,
  `language` varchar(16) NOT NULL,
  `score` int(11) NOT NULL,
  PRIMARY KEY (`course_id`,`forum_posts_id`,`language`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;"""

uselabcursor.execute(sql_create_coursera_message_language)

#list of dictionaries installed on this platform
languages=enchant.list_languages()
print "The following dictionaries will be used: " + str(languages)
#iterate over schemas
for schema_name in schemas:
	try:
		print "Working with schema " + str(schema_name)
		conn=get_connection(schema_name)
	except Exception, e:
		#chances are this database just doesn't exist, just move onto the next schema
		print "Error accessing " + str(schema_name) + " with exception " + str(e)
		continue
		
	for language in languages:
		print "Calculating spelling errors for language " + str(language)
		sql_insert_coursera_message_language="""INSERT INTO coursera_message_language (course_id,forum_posts_id,language,score) VALUES (%s,%s,%s,%s);"""
		try:
			chkr = SpellChecker(language)
		except:
			print "Problem with language, skipping"
			continue
		cursor=conn.cursor()
		cursor.execute("select id, post_text from forum_posts")
		for row in cursor:	
			chkr.set_text(row[1])
			cnt=0
			for err in chkr:
				cnt+=1
			values=(schema_name, row[0], language, cnt)
			uselabcursor.execute(sql_insert_coursera_message_language,values)
			uselabconn.commit()

conn.close()
uselabconn.close()
