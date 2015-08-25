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


from util.coursera_files import *
from util.coursera_db import *
import glob

import traceback

import os.path


s3_path = (get_properties().get('s3_path', None) + 'coursera_exports_sql/')
exports = (get_properties().get('sql',None))

set = ['fantasysf-005']
for schema in set:
    logger.info("Loading schema : {} \n".format(schema))
    file = ("{}*{}*.sql".format(exports, schema))
    files = glob.glob(file)
    schema = schema.replace('-', '_')
    (create_queries, copy_files) = print_sql(files, true, schema)
    connection = get_db_connection()

    connection.execute('\n'.join(create_queries))
    for (table) in copy_files:
        path = s3_path + ("{}.csv").format(schema + '.' + table)
        file_name = ("/tmp/{}.csv").format(schema + '.' + table)

        try:
            upload_s3_file(file_name,'coursera_exports_sql')
        except:
            logger.exception("This file : {} could not be uploaded to S3. Please update manually".format(file_name))
            logger.exception(traceback.format_exc(limit=None))

        try:
            copy_s3_to_redshift(connection, path, table, schema=schema, delim='|', error=10, ignoreheader=0)
        except:
            logger.exception("This table : {} could not be loaded from the file : {}. Please check pgcatalog.stl_load_errors".format(table,path ))
            logger.exception(traceback.format_exc(limit=None))

        else:
            os.remove(file_name)

        logger.info("Table successfully created".format(table))

    logger.info("File upload completed for schema : {} \n\n".format(schema))

