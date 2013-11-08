import pandas.io.sql as psql
import matplotlib.pyplot as plt
import argparse
from sqlconnection import *
from scipy.stats import ks_2samp

#parser = argparse.ArgumentParser(description='Create derivative statistics tables relating to discussion forums.')
#parser.add_argument('--schemas', help='An optional list of the schemas to create tables from', required=False)
#args = parser.parse_args()

uselabconn=get_connection("uselab_mooc")
uselabcursor=uselabconn.cursor()

#if (args.schemas != None):
#	schemas=args.schemas.split(",");
#else:
	#get a list of the databases to run this query over
sql_select_databases="select id from coursera_index;"
uselabcursor.execute(sql_select_databases);
schemas=[]
for row in uselabcursor:
	schemas.append(row[0].encode('ascii','ignore'))	

schemas=["sna-001","sna-003","insidetheinternet-002","fantasysf-001"]

for schema_name in schemas:
	try:
		conn=get_connection(schema_name)
	except Exception, e:
		print e
		continue
#	df=psql.read_frame("select count(*)/(select count(distinct(forum_user_id)) from `%s`.forum_posts) as cnt, forum_user_id from `%s`.forum_posts group by forum_user_id;"%(schema_name,schema_name),conn)
#	df=psql.read_frame("select count(*) as cnt, forum_user_id from `%s`.forum_posts group by forum_user_id;"%(schema_name),conn)
	df=psql.read_frame("select log(count(*)) as cnt, forum_user_id from `%s`.forum_posts group by forum_user_id;"%(schema_name),conn)
	fig=plt.figure()
	fig.suptitle(schema_name, fontsize=20)
	axes = fig.add_subplot(111)
	axes.xaxis.set_label("uniquegroups")
	axes.yaxis.set_label("# people")
	axes.hist(df.cnt, bins=200)
	plt.show()
	
"""

for schema_name1 in schemas:
	try:
		conn1=get_connection(schema_name1)
	except Exception, e:
		continue
	df1=psql.read_frame("select log(count(*)) as cnt, forum_user_id from `%s`.forum_posts group by forum_user_id;"%(schema_name1),conn1)
#	df1=psql.read_frame("select count(*) as cnt, forum_user_id from `%s`.forum_posts group by forum_user_id;"%(schema_name1),conn1)
#	df1=psql.read_frame("select count(*)/(select count(distinct(forum_user_id)) from `%s`.forum_posts) as cnt, forum_user_id from `%s`.forum_posts group by forum_user_id;"%(schema_name1,schema_name1),conn1)

	comparisons=schema_name1+" compared to:\n"
	for schema_name2 in schemas:
		try:
			conn2=get_connection(schema_name2)
		except Exception, e2:
			continue
		df2=psql.read_frame("select log(count(*)) as cnt, forum_user_id from `%s`.forum_posts group by forum_user_id;"%(schema_name2),conn2)
#		df2=psql.read_frame("select count(*) as cnt, forum_user_id from `%s`.forum_posts group by forum_user_id;"%(schema_name2),conn2)
#		df2=psql.read_frame("select count(*)/(select count(distinct(forum_user_id)) from `%s`.forum_posts) as cnt, forum_user_id from `%s`.forum_posts group by forum_user_id;"%(schema_name2,schema_name2),conn2)

		ks,p=ks_2samp(df1.cnt,df2.cnt)
		comparisons+="\t"
		if p < 0.01:
			comparisons+="* "
		else:
			comparisons+="  "
		comparisons+="{0:<30} ks={1:>.20} p={2:>.20} \n".format(schema_name2,str(ks),str(p))
	print comparisons+"\n"
"""