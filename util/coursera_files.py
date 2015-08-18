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

from util.config import *

import sys

import boto
import boto.s3.connection

logger = get_logger("coursera_files.py")


def get_s3_connection():
    conn_s3 = boto.s3.connect_to_region('us-east-1',
                                        aws_access_key_id=get_properties().get('access_id', None),
                                        aws_secret_access_key=get_properties().get('secret_key', None),
                                        is_secure=False,
                                        host='s3.amazonaws.com',
                                        calling_format=boto.s3.connection.OrdinaryCallingFormat(),
                                        )
    return conn_s3


def str_contains(s, sub, exact=True):
    sub = sub.lower()
    if exact:
        return str_contains(s, " " + sub + " ", exact=False) or str_contains(s, " " + sub + "(", exact=False)
    else:
        return not s.find(sub, 0) == -1


def if_str_contains_then_replace(s, sub, repl, exact=True):
    sub = sub.lower()
    if (exact and (
        str_contains(s, " " + sub + " ", exact=False) or str_contains(s, " " + sub + "(", exact=False))) or not s.find(
            sub, 0) == -1:
        # sub was found
        return repl
    return s


def parse_column(line):
    tokens = line.strip()[:-1].split()
    col_name = tokens[0].replace("`", "")
    col_type = tokens[1]
    # we ignore not null and default values

    #print(col_type.split("(")[0])
    # from https://www.flydata.com/resources/flydata-sync/data-type-mapping/
    #print(line)
    col_type = if_str_contains_then_replace(col_type, "BINARY", "varchar")
    col_type = if_str_contains_then_replace(col_type, "BIT", "int8")
    col_type = if_str_contains_then_replace(col_type, "BLOB", "varchar(65535)")
    col_type = if_str_contains_then_replace(col_type, "BOOL", "int2")
    col_type = if_str_contains_then_replace(col_type, "CHAR", "varchar")
    col_type = if_str_contains_then_replace(col_type, "DATE", "date")
    col_type = if_str_contains_then_replace(col_type, "TIME", "timestamp")
    col_type = if_str_contains_then_replace(col_type, "DEC", "numeric")
    col_type = if_str_contains_then_replace(col_type, "DOUBLE", "float8")
    col_type = if_str_contains_then_replace(col_type, "ENUM", "varchar")
    col_type = if_str_contains_then_replace(col_type, "FIXED", "numeric")
    col_type = if_str_contains_then_replace(col_type, "FLOAT", "float4")
    col_type = if_str_contains_then_replace(col_type, "INT", "int8")
    col_type = if_str_contains_then_replace(col_type, "LONGTEXT", "varchar(max)")
    col_type = if_str_contains_then_replace(col_type, "TEXT", "varchar")
    col_type = if_str_contains_then_replace(col_type, "NUMERIC", "numeric")
    col_type = if_str_contains_then_replace(col_type, "SET", "varchar")
    col_type = if_str_contains_then_replace(col_type, "YEAR", "date")
    #print(col_type)

    # Rename the column if it is now a reserved word
    if col_name == "default" or col_name == "order" or col_name == "ignore":
        col_name = "reserved_{}".format(col_name)
    # print(col_type)
    return (col_name, col_type)


def convert_create_statement(stmt, local_tables, other_tables):
    # get the column definitions
    table_name = ''
    columns = []
    keys = []
    for line in stmt.split("\n"):
        if line.startswith("CREATE TABLE"):
            table_name = line.split()[2].replace('`', '')
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
    create_stmt = "CREATE TABLE {}(".format(table_name)
    for col in columns:
        create_stmt += "\n\t{} {},".format(col[0], col[1])
    create_stmt = create_stmt[:-1] + ");"
    if table_name.find(".", 0) != -1:
        other_tables.append(create_stmt)
    else:
        local_tables.append(create_stmt)


def print_sql(files, clean, schema):
    local_tables = []
    other_tables = []
    insert_statements = []
    query = []

    if schema is None:
        try:
            schema = files[0].split("(")[1].split(")")[0]
        except:
            print("Unable to detect schema for tables from filename, please specific with --schema")
            exit(-1)

    schema = schema.replace('-', '_')

    for f in files:
        with open(f) as fil:
            cur_create = None
            for line in fil:
                if line.startswith("CREATE TABLE") and cur_create is None:
                    cur_create = line
                elif line.startswith("CREATE TABLE") and cur_create is not None:
                    print("Error, line starts with create table but cur_create is not none")
                elif line.endswith(";\n") and cur_create is not None:
                    cur_create += line
                    convert_create_statement(cur_create, local_tables, other_tables)
                    cur_create = None
                elif cur_create is not None:
                    cur_create += line
                elif line.startswith("INSERT INTO") and cur_create is None:
                    table_name = line.split()[2]
                    ins_table_name = line.split()[2].replace('`', '').replace(".", "_")
                    line = line.replace(table_name, ins_table_name).replace("%", "%%").replace("TABLES", "TABLE")
                    insert_statements.append(line)

    if clean:
        query.append("DROP SCHEMA IF EXISTS {};".format(schema))

    query.append("CREATE SCHEMA {};".format(schema))
    query.append("SET search_path TO {};".format(schema))

    for table in local_tables:
        query.append(table)

    for table in other_tables:
        # These tables have periods in the name.  Is this meant to put them in a new database?  A new schema?  Some of the
        # tables have multiple periods in the name.  At the moment the strategy here is to replace the periods with an
        # underscore so they can be put into redshift
        table = table.replace(".", "_")
        query.append(table)

    for line in insert_statements:
        query.append(line)

    return '\n'.join(query)


def upload_files_to_s3(type):
    # source directory
    sourceDir = get_properties().get(type, None)
    # destination directory name (on s3)
    destDir = [dir for dir in sourceDir.split('/') if dir != ''][-1]

    upload_s3_dir(sourceDir, destDir)


def upload_s3_dir(sourceDir, destDir):
    uploadFileNames = []
    for (sourceDir, dirname, filename) in os.walk(sourceDir):
        uploadFileNames.extend(filename)
        break

    for filename in uploadFileNames:
        sourcepath = os.path.join(sourceDir + filename)
        upload_s3_file(sourcepath, destDir)


def upload_s3_file(source_filename, dest_dir):
    bucket = get_s3_connection().get_bucket(get_properties().get('bucket', None), validate=False)
    # to avoid cost of listing a bucket and since we use only one bucket, validation is turned OFF

    # max size in bytes before uploading in parts. between 1 and 5 GB recommended
    MAX_SIZE = 20 * 1000 * 1000
    # size of parts when uploading in parts
    PART_SIZE = 6 * 1000 * 1000

    filename = [dir for dir in source_filename.split('/') if dir != ''][-1]
    destpath = os.path.join(dest_dir, filename)

    logger.info('Uploading file : {} to Amazon S3 bucket'.format(filename))

    def percent_cb(complete, total):
        sys.stdout.write('.')
        sys.stdout.flush()

    filesize = os.path.getsize(source_filename)
    if filesize > MAX_SIZE:
        logger.info("Multipart upload")
        mp = bucket.initiate_multipart_upload(destpath)
        fp = open(source_filename, 'rb')
        fp_num = 0
        while (fp.tell() < filesize):
            fp_num += 1
            logger.info("Uploading part no. {}".format(fp_num))
            mp.upload_part_from_file(fp, fp_num, cb=percent_cb, num_cb=10, size=PART_SIZE)
            mp.complete_upload()

    else:
        logger.info("Singlepart upload")
        k = boto.s3.key.Key(bucket)
        k.key = destpath
        k.set_contents_from_filename(source_filename,
                                     cb=percent_cb, num_cb=10)
