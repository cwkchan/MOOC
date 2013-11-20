#    Copy pii data from csv to SQL database.
#    Copyright (C) 2013  Eric Koo <erickoo@umich.edu>
#    Copyright (C) 2013  Christopher Brooks <brooksch@umich.edu>
#    USE Lab, Digital Media Commons, University of Michigan Library
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
from sqlconnection import *

parser = argparse.ArgumentParser(description='Copy pii data from csv to SQL database.')
parser.add_argument('--clean', action='store_true', help='Whether to drop tables in the database or not')
parser.add_argument('--csvs', help='A list of csv files to create tables from', required=True)
args = parser.parse_args()

uselabconn = get_connection('uselab_mooc')
uselabcursor = uselabconn.cursor()

if (args.clean):
  sql_drop_coursera_pii = '''DROP TABLE IF EXISTS `coursera_pii`;'''
  uselabcursor.execute(sql_drop_coursera_pii)

sql_create_coursera_pii = '''
    CREATE TABLE IF NOT EXISTS `coursera_pii` (
    `user_id` INT NOT NULL,
    `session_id` VARCHAR(255) NOT NULL,
    `name` VARCHAR(255) CHARACTER SET utf32 DEFAULT NULL,
    `email` VARCHAR(255) DEFAULT NULL,
    PRIMARY KEY (`user_id`, `session_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
    '''
uselabcursor.execute(sql_create_coursera_pii)

csvs = args.csvs.split(',')

for csv_file in csvs:
  with open(csv_file,'rU') as IN:
    sql_insert_into_coursera_pii = '''INSERT INTO coursera_pii (user_id, session_id, name, email) VALUES (%s,%s,%s,%s);'''
    for row in IN:
      row = row.strip()            # get rid of trailing \n
      row = row.replace('"',"'")   # change " to '
      row = row.split(';')
      values = (row[0],csv_file.replace('.csv',''),row[1],row[2])
      uselabcursor.execute(sql_insert_into_coursera_pii, values)
      uselabconn.commit()

uselabconn.close()
