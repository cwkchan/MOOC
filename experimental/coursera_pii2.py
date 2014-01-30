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
group.add_argument('--dir', help='A directory with CSV files in it')
args = parser.parse_args()

logger = get_logger("coursera_clickstream.py", args.verbose)
conn = get_connection()

if args.clean:
    query = """DROP TABLE IF EXISTS `coursera_pii2`"""
    try:
        conn.execute(query)
    except:
        pass

try:
    query = """
        CREATE TABLE IF NOT EXISTS coursera_pii2 (
            session_id VARCHAR(255) NOT NULL,
            coursera_user_id INT NOT NULL,
            session_user_id VARCHAR(255) NOT NULL,
            forum_user_id VARCHAR(255) NOT NULL,
            email_address VARCHAR(255) DEFAULT NULL,
            full_name VARCHAR(255) CHARACTER SET utf32 DEFAULT NULL,
            first_name VARCHAR(255) CHARACTER SET utf32 DEFAULT NULL,
            middle_name VARCHAR(255) CHARACTER SET utf32 DEFAULT NULL,
            last_name VARCHAR(255) CHARACTER SET utf32 DEFAULT NULL,
            name_cleaning_confidence float DEFAULT NULL,
            PRIMARY KEY (session_id, coursera_user_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
        """
    conn.execute(query)
except:
    pass

# Check which sessions are already in the database
query = """SELECT DISTINCT session_id FROM coursera_pii2;"""
existing = []
for row in conn.execute(query):
  existing.append(str(row['session_id']))

for csv in listdir(args.dir):
    session_id = filename_to_schema(csv)
    if session_id not in existing:
        logger.debug("Reading file {} into dataframe.".format(csv))
        try:
            df = pd.io.parsers.read_csv(args.dir+'/'+csv)
        except Exception, e:
            logger.warn("Exception found, skipping this file: {}".format(e))
            continue

        try:
            # boo, workaround for bug https://github.com/pydata/pandas/issues/2754
            # convert the dataframe to a single type to replace nulls with Nones
            df = df.astype(object)
            # SQL wants None, not NaN
            df[pd.isnull(df)] = None
            #df.where(pd.notnull(df), None)

            df['session_id'] = session_id
            # clean the names, this was not immediately clear to me, kudos to:
            # http://stackoverflow.com/questions/12356501/pandas-create-two-new-columns-in-a-dataframe-with-values-calculated-from-a-pre
            df['first_name'], df['middle_name'], df['last_name'], df['name_cleaning_confidence'] = zip(*df['full_name'].map(clean))
            df = df[['session_id', 'coursera_user_id', 'session_user_id', 'forum_user_id', 'email_address', 'full_name', 'first_name', 'middle_name', 'last_name', 'name_cleaning_confidence']]

            logger.debug("Writing data from file {} into database.".format(csv))
            pd.io.sql.write_frame(df, 'coursera_pii2', conn.raw_connection(), flavor='mysql', if_exists='append')
        except Exception, e:
            logger.warn("Exception {}".format(e))
