import pandas.io.sql as psql
import argparse
from sqlconnection import *
from scipy.stats import ks_2samp

parser = argparse.ArgumentParser(description='Create derivative statistics tables relating to discussion forums.')
parser.add_argument('--schemas', help='An optional list of the schemas to create tables from', required=False)
args = parser.parse_args()

uselabconn=get_connection("uselab_mooc")
uselabcursor=uselabconn.cursor()

if (args.schemas != None):
	schemas=args.schemas.split(",");
else:
	#get a list of the databases to run this query over
	sql_select_databases="select id from coursera_index;"
	uselabcursor.execute(sql_select_databases);
	schemas=[]
	for row in uselabcursor:
		schemas.append(row[0].encode('ascii','ignore'))	

for schema_name1 in schemas:
	try:
		conn1=get_connection(schema_name1)
	except Exception, e:
		continue
	df1=psql.read_frame("select count(*) as cnt from forum_posts group by thread_id;",conn1)

	comparisons=schema_name1+" compared to:\n"
	for schema_name2 in schemas:
		try:
			conn2=get_connection(schema_name2)
		except Exception, e2:
			continue
		df2=psql.read_frame("select count(*) as cnt from forum_posts group by thread_id;",conn2)

		ks,p=ks_2samp(df1.cnt,df2.cnt)
		comparisons+="\t"
		if p < 0.01:
			comparisons+="* "
		else:
			comparisons+="  "
		comparisons+="{0:<30} ks={1:>.20} p={2:>.20} \n".format(schema_name2,str(ks),str(p))
	print comparisons+"\n"