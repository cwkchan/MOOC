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

from util.config import *

import argparse

parser = argparse.ArgumentParser(description='Create views across schemas')
parser.add_argument('--clean', action='store_true', help='Whether to drop views first')
parser.add_argument('--columns', action='store', default="*",
                    help='The name of the columns to pull from each schema, defaults to all')
parser.add_argument('--include-self', action='store_true',
                    help='If this flag exists the table a row comes from will be stored in column "source"')
parser.add_argument('--materialized', action='store_true', help='If True the view will be created as a base table')
parser.add_argument('--schemas', action='store',
                    help='An optional list of schemas to consider, defaults to all those in the coursera_index table')
parser.add_argument('--table', action='store', required=True, help='The name of the table within each schema')
parser.add_argument('--verbose', action='store_true', help='If this flag exists extended logging will be on')
args = parser.parse_args()

logger = get_logger("cross_schema_views.py", args.verbose)

conn = get_connection()
if args.clean:
    query = ""
    if args.materialized:
        query = "DROP TABLE IF EXISTS cross_{}".format(args.table)
    else:
        query = "DROP VIEW IF EXISTS cross_{}".format(args.table)
    try:
        conn.execute(query)
    except:
        pass

query = ""

schemas = []
if args.schemas:
    schemas = args.schemas.split(",")
else:
    schemas = get_coursera_schema_list()

for schema_name in schemas:
    #check to see if the source should be added
    subquery = ""
    if args.include_self:
        sub_query = "SELECT {}, '{}' SOURCE FROM `{}`.{}".format(args.columns, schema_name, schema_name, args.table)
    else:
        sub_query = "SELECT {} FROM `{}`.{}".format(args.columns, schema_name, args.table)
    query = query + " UNION " + sub_query

if args.materialized:
    query = "CREATE TABLE cross_{} AS {};".format(args.table, query[7:])
else:
    query = "CREATE OR REPLACE VIEW cross_{} as {};".format(args.table, query[7:])

logging.info("Preparing query: {}".format(query))

try:
    conn.execute(query)
except Exception as e:
    logger.info("Error running query, exception: {}".format(e))