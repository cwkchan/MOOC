# Copyright (C) 2013 The Regents of the University of Michigan
#
# This program is free software: you can redistribute it and/or modify
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
from sqlalchemy import *

logger = get_logger("coursera_db.py")

def get_db_connection(schema=None):
    """Returns an sqlalchemy connection object, optionally connecting to a particular schema of interest.  If no schema
    is used, the one marked as the index in the configuration file will be used. """
    config = get_properties()
    if (schema == None):
        return create_engine(config["engine"] + config["schema"]).connect()
    return create_engine(config["engine"] + schema).connect()

def get_coursera_schema_list():
    """Returns the list of courses, as db schemas, that should be processed. This list comes from either the
    configuration file in the schemas section or, if that does not exist, it comes from the coursera_index table."""
    config = get_properties()
    if ("schemas" in config.keys()):
        return config["schemas"].split(",")

    query = "SELECT session_id FROM coursera_index WHERE start IS NOT NULL;"
    conn = get_db_connection()
    results = conn.execute(query)
    schemas = []
    for row in results:
        schemas.append(row["session_id"])
    return schemas

def convert_sessionid_to_id(session_id):
    """Converts a session id in the form of 'introfinance-001' to a primary key such as 2.  Simple wrapper to select
    from the uselab_mooc.coursera_index table"""
    conn = get_db_connection()
    rs = conn.execute("SELECT admin_id FROM coursera_index WHERE session_id LIKE '{}'".format(session_id))
    id = int(rs.fetchone()[0])
    return id

#adapted from http://stackoverflow.com/questions/28271049/redshift-copy-operation-doesnt-work-in-sqlalchemy
def copy_s3_to_redshift(conn, s3path, table, schema= None, delim='\t', error=50, ignoreheader=1):
    aws_access_key = get_properties().get('access_id', None)
    aws_secret_key = get_properties().get('secret_key', None)

    if (schema is None):
        schema = "public"

    copy = text("""
        copy "{table}"
        from :s3path
        credentials 'aws_access_key_id={aws_access_key};aws_secret_access_key={aws_secret_key}'
        emptyasnull
        ignoreheader :ignoreheader
        compupdate on
        delimiter :delim
        maxerror :error
        removequotes
        NULL AS 'null'
        ;
        """.format(table=text(table), aws_access_key=aws_access_key, aws_secret_key=aws_secret_key, error=error))    # copy command doesn't like table name or keys single-quoted

    logger.info("Copying the file : {} from S3 to table : {}.{} ".format(s3path, schema, table))
    trans = conn.begin()
    conn.execute("SET search_path TO {};".format(schema))
    conn.execute(copy, s3path=s3path, delim=delim, ignoreheader=ignoreheader or 0, error=error)
    trans.commit()



