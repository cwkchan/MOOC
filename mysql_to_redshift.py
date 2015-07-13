# Copyright (C) 2013  The Regents of the University of Michigan
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see [http://www.gnu.org/licenses/].

import argparse
import glob
import sys

parser = argparse.ArgumentParser(description='Print out table definitions for redshift based on mysql source')
parser.add_argument('--verbose', action='store_true', help='Whether to debug log or not')
parser.add_argument('--files', action='store', help="The file to parse")
parser.add_argument('--clean', action='store_true', help="Whether to issue a drop schema statement first or not")
parser.add_argument('--schema', action='store', help="The schema to create the tables in, if not exists then the parser tries to guess from the filenames.")
args = parser.parse_args()

if args.files is None:
    print("You must specify a file or set of files with a wildcard.  E.g. *.sql")
    sys.exit(-1)
else:
    args.files=glob.glob(args.files)

if args.schema is None:
    try:
        args.schema=args.files[0].split("(")[1].split(")")[0]
    except:
        print("Unable to detect schema for tables from filename, please specific with --schema")
        sys.exit(-1)

local_tables=[]
other_tables=[]

def str_contains(s, sub, exact=True):
    sub=sub.lower()
    if exact:
        return str_contains(s, " " + sub + " ", exact=False) or str_contains(s, " " +sub +"(", exact=False)
    else:
        return not s.find(sub, 0) == -1

def if_str_contains_then_replace(s,sub,repl,exact=True):
    sub=sub.lower()
    if (exact and (str_contains(s, " " + sub + " ", exact=False) or str_contains(s, " " +sub +"(", exact=False))) or not s.find(sub, 0) == -1:
        #sub was found
        return repl
    return s

def parse_column(line):
    tokens = line.strip()[:-1].split()
    col_name = tokens[0].replace("`","")
    col_type = tokens[1]
    # we ignore not null and default values

    #print(col_type.split("(")[0])
    #from https://www.flydata.com/resources/flydata-sync/data-type-mapping/
    #print(line)
    col_type=if_str_contains_then_replace(col_type,"BINARY","varchar")
    col_type=if_str_contains_then_replace(col_type,"BIT","int8")
    col_type=if_str_contains_then_replace(col_type,"BLOB","varchar(65535)")
    col_type=if_str_contains_then_replace(col_type,"BOOL","int2")
    col_type=if_str_contains_then_replace(col_type,"CHAR","varchar")
    col_type=if_str_contains_then_replace(col_type,"DATE","date")
    col_type=if_str_contains_then_replace(col_type,"TIME","timestamp")
    col_type=if_str_contains_then_replace(col_type,"DEC","numeric")
    col_type=if_str_contains_then_replace(col_type,"DOUBLE","float8")
    col_type=if_str_contains_then_replace(col_type,"ENUM","varchar")
    col_type=if_str_contains_then_replace(col_type,"FIXED","numeric")
    col_type=if_str_contains_then_replace(col_type,"FLOAT","float4")
    col_type=if_str_contains_then_replace(col_type,"INT","int8")
    col_type=if_str_contains_then_replace(col_type,"TEXT","varchar")
    col_type=if_str_contains_then_replace(col_type,"NUMERIC","numeric")
    col_type=if_str_contains_then_replace(col_type,"SET","varchar")
    col_type=if_str_contains_then_replace(col_type,"YEAR","date")

    #Rename the column if it is now a reserved word
    if col_name=="default" or col_name=="order" or col_name=="ignore":
        col_name="reserved_{}".format(col_name)
    #print(col_type)
    return (col_name, col_type)


def convert_create_statement(stmt):
    # get the column definitions
    table_name = ''
    columns = []
    keys = []
    for line in stmt.split("\n"):
        if line.startswith("CREATE TABLE"):
            table_name = line.split()[2].replace('`','')
            continue
        if line.startswith(")") and line.endswith(";"):
            break
        if line.lstrip().startswith("PRIMARY KEY"):
            keys.append(line.split("(")[1].split(")")[0])
            continue
        if line.lstrip().startswith("KEY"):
            keys.append(line.split("(")[1].split(")")[0])
            continue
        if line.lstrip().startswith("`"):
            columns.append(parse_column(line))
    create_stmt="CREATE TABLE {}(".format(table_name)
    for col in columns:
        create_stmt+="\n\t{} {},".format(col[0],col[1])
    create_stmt=create_stmt[:-1]+");"
    if table_name.find(".",0)!=-1:
        other_tables.append(create_stmt)
    else:
        local_tables.append(create_stmt)


for f in args.files:
    with open(f) as fil:
        cur_create = None
        for line in fil:
            if line.startswith("CREATE TABLE") and cur_create is None:
                cur_create = line
            elif line.startswith("CREATE TABLE") and cur_create is not None:
                print("Error, line starts with create table but cur_create is not none")
            elif line.endswith(";\n") and cur_create is not None:
                cur_create += line
                convert_create_statement(cur_create)
                cur_create = None
            elif cur_create is not None:
                cur_create += line

if args.clean:
    print("DROP SCHEMA IF EXISTS {};".format(args.schema))

print("CREATE SCHEMA {};".format(args.schema))
print("SET search_path TO {};".format(args.schema))

for table in local_tables:
    print(table)

for table in other_tables:
    # These tables have periods in the name.  Is this meant to put them in a new database?  A new schema?  Some of the
    # tables have multiple periods in the name.  At the moment the strategy here is to replace the periods with an
    # underscore so they can be put into redshift
    table=table.replace(".","_")
    print(table)
