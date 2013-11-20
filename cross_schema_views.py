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

parser = argparse.ArgumentParser(description='Create views across schemas')
parser.add_argument('--table', action='store', help='The name of the table within each schema')
parser.add_argument('--columns', action='store', help='The name of the columns to pull from each schema')
parser.add_argument('--verbose', action='store_true', help='If this flag exists extended logging will be on')
args = parser.parse_args()

logger=get_logger("cross_schema_views.py",args.verbose)

query=""
schemas=get_coursera_schema_list();
for schema_name in schemas:
	sub_query="""SELECT {} FROM `{}`.{}""".format(args.columns,schema_name,args.table)
	query=query+" UNION "+sub_query
query="CREATE OR REPLACE VIEW cross_{} as {};".format( args.table,query[7:] )

logging.info("Preparing query: {}".format(query))

try:
	conn=get_connection()
	conn.execute(query)
except Exception, e:
	logger.warn("Error running query, exception: {}".format(e))