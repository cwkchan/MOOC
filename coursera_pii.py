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
from names.clean_names import *

parser = argparse.ArgumentParser(description='Copy pii data from csv to SQL database.')
parser.add_argument('--clean', action='store_true', help='Whether to drop tables in the database or not')
parser.add_argument('--verbose', action='store_true', help='Whether to debug log or not')
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--csvs', help='A list of csv files to create tables from')
group.add_argument('--dir', help='A directory with CSV files in it')
args = parser.parse_args()

logger=get_logger("coursera_clickstream.py",args.verbose)
conn=get_connection()

if (args.clean):
	query="""DROP TABLE IF EXISTS `coursera_pii`"""
	try:
		conn.execute(query)
	except:
		pass
		
query='''CREATE TABLE IF NOT EXISTS coursera_pii (
    user_id INT NOT NULL,
    session_id VARCHAR(255) NOT NULL,
    name VARCHAR(255) CHARACTER SET utf32 DEFAULT NULL,
    email VARCHAR(255) DEFAULT NULL,
		`first_name` VARCHAR(255) CHARACTER SET utf32 DEFAULT NULL,
		`middle_name` VARCHAR(255) CHARACTER SET utf32 DEFAULT NULL,
		`last_name` VARCHAR(255) CHARACTER SET utf32 DEFAULT NULL,
		`name_cleaning_confidence` float DEFAULT NULL,
    PRIMARY KEY (user_id, session_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
    '''
conn.execute(query)

if (args.dir!=None):
	csvs = listdir(args.dir)
	csvs=map(lambda x:args.dir+x,csvs)
else:
	csvs = args.csvs.split(',')

for csv_file in csvs:
  session_id = filename_to_schema(csv_file)
  logger.debug("Reading file {} into dataframe.".format(csv_file))
  try:
  	df = pd.io.parsers.read_csv(csv_file, delimiter=';')
  except Exception, e:
  	logger.warn("Exception found, skipping this file: {}".format(e))
  	continue
  df.columns = ['user_id', 'name', 'email']
  df['session_id'] = session_id
  df = df[['user_id', 'session_id', 'name', 'email']]
  
  #clean the names
  #this was not immediatly clear to me, kudos to:
  #http://stackoverflow.com/questions/12356501/pandas-create-two-new-columns-in-a-dataframe-with-values-calculated-from-a-pre
  df["first_name"],df["middle_name"],df["last_name"],df["name_cleaning_confidence"]=zip(*map(clean, df["name"]))

  #should we be doing this if they didn't say --clean?
  try:
  	sql_delete_session_id = '''DELETE FROM coursera_pii WHERE session_id='{}';'''.format(session_id)
  	conn.execute(sql_delete_session_id)
  	#boo, workaround for bug https://github.com/pydata/pandas/issues/2754
  	df[df.columns] = df[df.columns].astype(object) #convert the dataframe to a single type to replaces nulls with Nones
  	df[pd.isnull(df)] = None #SQL wants None, not NaN
  	logger.debug("Writing data from file {} into database.".format(csv_file))
  	pd.io.sql.write_frame(df, 'coursera_pii', conn.raw_connection(), flavor='mysql', if_exists='append')
  except Exception, e:
		logger.warn("Exception {}".format(e))