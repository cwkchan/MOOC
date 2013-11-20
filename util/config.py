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

import mysql.connector
import ConfigParser
from sqlalchemy import *
import logging

def get_properties():
	"""Returns the list of properties as a dict of key/value pairs in
	the file config.properties.
	"""
	cf = ConfigParser.ConfigParser()
	cf.read("config.properties")
	properties={}
	for section in cf.sections():
		for item in cf.items(section):
			properties[item[0]]=item[1]
	return properties

def get_connection(schema=None):
	"""Returns an sqlalchemy connection object, optionally connecting to
	a particular schema of interest.  If no schema is used, the one marked
	as the index in the configuration file will be used.
	"""
	config=get_properties()
	if (schema == None):
		return create_engine(config["engine"]+config["schema"])
	return create_engine(config["engine"]+schema)

def get_coursera_schema_list():
	"""Returns the list of courses, as db schemas, that should be processed.
	This list comes from either the configuration file in the schemas section
	or, if that does not exist, it comes from the coursera_index table.
	"""
	config=get_properties()
	if ("schemas" in config.keys()):
		return config["schemas"].split(",")
	
	query="SELECT id FROM coursera_index WHERE start IS NOT NULL;"
	conn=get_connection()
	results=conn.execute(query)
	schemas=[]
	for row in results:
		schemas.append(row["id"].encode('ascii','ignore'))
	return schemas

def get_logger(name,verbose=False):
	logger = logging.getLogger(name)
	if verbose:
		logging.basicConfig(level=logging.DEBUG)
	else:
		logging.basicConfig(level=logging.INFO)
	return logger
	
