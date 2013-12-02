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

parser = argparse.ArgumentParser(description='Copy sections_blocks tables from csv to SQL database.')
parser.add_argument('--clean', action='store_true', help='Whether to drop tables in the database or not')
parser.add_argument('--verbose', action='store_true', help='Whether to debug log or not')
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--dir', help='A directory with CSV files in it')
args = parser.parse_args()

logger = get_logger("coursera_clickstream.py",args.verbose)
conn = get_connection()

if (args.clean):
  query="""DROP TABLE IF EXISTS coursera_map_sections_blocks;"""
  try:
    conn.execute(query)
  except:
    pass

query = """
  CREATE TABLE IF NOT EXISTS coursera_map_sections_blocks (
    session_id VARCHAR(255) NOT NULL,
    section_id INT(11) NOT NULL,
    block_id INT(11) NOT NULL,
    PRIMARY KEY (session_id, section_id)
  ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
  """
conn.execute(query)

# Check which sessions are already in the database
query = """SELECT DISTINCT session_id FROM coursera_map_sections_blocks;"""
existing = []
for row in conn.execute(query):
  existing.append(str(row['session_id']))

for csv in listdir(args.dir):
  session_id = filename_to_schema(csv)
  if session_id not in existing:
    df = pd.io.parsers.read_csv(args.dir+'/'+csv)
    df['session_id'] = filename_to_schema(csv)
    pd.io.sql.write_frame(df, 'coursera_map_sections_blocks', conn.raw_connection(), flavor='mysql', if_exists='append')
