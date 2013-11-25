import matplotlib.pyplot as plt
import pandas as pd
from sqlconnection import *


sql="select id, UNIX_TIMESTAMP(start) as start ,UNIX_TIMESTAMP(end) as end from uselab_mooc.coursera_index where start is not null;"
uselabcursor=get_connection("uselab_mooc").cursor();
uselabcursor.execute(sql);
schemas=[]
for row in uselabcursor:
	schemas.append((row[0].encode('ascii','ignore'),row[1],row[2]))	

sql="""select 
    avg(s.min_age) as age, avg(s.word_count) as cnt
from
    uselab_mooc.coursera_message_stats s,
    `%s`.forum_posts p
where
    s.course_id = '%s'
        and p.id = s.forum_posts_id and p.post_time between %s and %s"""

conn=get_connection("uselab_mooc")
cur=conn.cursor()
for schema_name,start,end in schemas:
	cur_time=start
	age_averages=[]
	word_count_averages=[]
	while (cur_time < end):
		width=60*60*24 #one day
		cur.execute(sql%(schema_name,schema_name,cur_time,cur_time+width))
		cur_time=cur_time+width
		res=cur.fetchone()
		age_averages.append(res[0])
		word_count_averages.append(res[1])
	fig=plt.figure()
	fig.suptitle(schema_name, fontsize=20)
	axes = fig.add_subplot(111)
	axes.plot(age_averages)
	axes.plot(word_count_averages)
	plt.show()
	
"""	

	age_averages.append()
	word_count_average.append(cnt)
	
for schema_name,start,end in schemas:
	df=pd.io.sql.read_frame(sql%(schema_name,schema_name),get_connection("uselab_mooc"))
	width=60*60*24 #one day
	fig=plt.figure()
	fig.suptitle(schema_name, fontsize=20)
	axes = fig.add_subplot(111)
	axes.plot(df.post_time,df.min_age)
	#axes.xaxis.set_label("Time in ms")
	#axes.yaxis.set_label("# messages posted")
	#axes.hist(df.post_time,bins=range(start,end+width,width))
	#plot weekly lines
	for x in range(start, end, width*7):
		axes.axvline(x,color='r')
	plt.show()
	"""