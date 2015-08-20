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
from util.coursera_files import *
from util.coursera_db import *
import glob
import boto
import boto.s3
import traceback

import os.path
from os.path import basename
import sys

# Fill in info on data to upload
# destination bucket name
#bucket_name = 'mooc-storage'
# source directory
#sourceDir = get_properties().get('pii', None)
# destination directory name (on s3)
#destDir = [dir for dir in sourceDir.split('/') if dir != ''][-1]
#destDir = 'coursera_demographics'
# os.path.dirname(sourceDir).split('/')
#upload_s3_dir(sourceDir, destDir)
s3_path = (get_properties().get('s3_path', None) + 'coursera_exports_sql/')

set = ['aidsfearandhope-001',
        'cataractsurgery-001',
        'cataractsurgery-002',
        'clinicalskills-001',
        'clinicalskills-002',
        'digitaldemocracy-002',
        'digitaldemocracy-003',
        'digitaldemocracy-2012-001',
        'fantasysf-002',
        'fantasysf-003',
        'fantasysf-004',
        'fantasysf-005',
        'fantasysf-006',
        'fantasysf-007',
        'fantasysf-008',
        'fantasysf-2012-001',
        'insidetheinternet-002',
        'insidetheinternet-003',
        'insidetheinternet-004',
        'insidetheinternet-005',
        'insidetheinternet-006',
        'insidetheinternet-007',
        'insidetheinternet-008',
        'insidetheinternet-2012-001',
        'instructmethodshpe-001',
        'instructmethodshpe-002',
        'instructmethodshpe-003',
        'instructmethodshpe-004',
        'instructmethodshpe-005',
        'introfinance-002',
        'introfinance-003',
        'introfinance-004',
        'introfinance-005',
        'introfinance-006',
        'introfinance-007',
        'introfinance-008',
        'introfinance-2012-001',
        'introthermodynamics-001',
        'introthermodynamics-002',
        'introthermodynamics-003',
        'introthermodynamics-004',
        'introthermodynamics-005',
        'modelthinking',
        'modelthinking-003',
        'modelthinking-004',
        'modelthinking-005',
        'modelthinking-006',
        'modelthinking-007',
        'modelthinking-008',
        'modelthinking-009',
        'modelthinking-2012-002',
        'modelthinkingzh-001',
        'Python',
        'pythonlearn-001',
        'pythonlearn-002',
        'pythonlearn-003',
        'pythonlearn-004',
        'questionnairedesign-001',
        'questionnairedesign-002',
        'questionnairedesign-003',
        'sna-002',
        'sna-003',
        'sna-004',
        'sna-2012-001',
        'successfulnegotiation-001',
        'ushealthcare-001']

for schema in set:
    file = ("/home/purva/coursera_sql_exports/*{}*.sql".format(schema))
    files = glob.glob(file)
    schema = schema.replace('-', '_')
    (create_queries, copy_files) = print_sql(files, true, schema)
    connection = get_db_connection()

    connection.execute('\n'.join(create_queries))
    print('\t'.join(copy_files))

    for (table) in copy_files:
        path = s3_path + ("{}.csv").format(schema + '.' + table)
        file_name = ("/tmp/{}.csv").format(schema + '.' + table)
        print(path)

        try:
            upload_s3_file(file_name,'coursera_exports_sql')
        except:
            print("This file : {} could not be uploaded to S3. Please update manually".format(file_name))
            traceback.print_exception()

        try:
            copy_s3_to_redshift(connection, path, table, schema=schema, delim='|', error=10, ignoreheader=0)
        except:
            print("This table : {} could not be loaded from the file : {}. Please check pgcatalog.stl_load_errors".format(table,path ))
            traceback.print_exception()
        else:
            os.remove(file_name)
