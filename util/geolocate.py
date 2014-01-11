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
import maxminddb
from config import *
import sys
from sqlalchemy import *
from sqlalchemy.exc import *

def __get_nested_item(item, dikt):
	"""Gets an item out of a nested dictionary of items, or None if it
	doesn't exist.  We are aiming to deal with this JSON data quickly:
	http://dev.maxmind.com/geoip/geoip2/web-services"""
	try:
		for key in item:
			if type(dikt) == list:
				#todo: the list could have more than one subdivision in it, handle that in a normalized schema
				dikt=dikt[0]
			dikt=dikt[key]
		return dikt
	except:
		return None

parser = argparse.ArgumentParser(description='Create tables to map IP addresses to countries.  This script can be called directly, but is intended to be called by other python scripts in this package.  The result of running this script is that IP address details are populated in the coursera_geolocate table.  For instance, if you wanted to call this script on all IP addresses from the coursera last_ip value in all of your schemas you might use: python ./geolocate.py  --clean --verbose --sql="SELECT DISTINCT last_access_ip FROM users WHERE last_access_ip IS NOT NULL AND last_access_ip NOT LIKE \'\'"')
parser.add_argument('--clean', action='store_true', help='Whether to drop tables in the database or not')
parser.add_argument('--verbose', action='store_true', help='If this flag exists extended logging will be on')
parser.add_argument('--schemas', action='store', help='The comma separated list of schemas to run the query on, defaults to values in configuration file or coursera_index table')
parser.add_argument('--sql', action='store', required="True", help='The query to run, must return list of IP addresses.  E.g. SELECT DISTINCT last_access_ip FROM users WHERE last_access_ip IS NOT NULL AND last_access_ip NOT LIKE ""' )
args = parser.parse_args()

logger=get_logger("geolocate.py",args.verbose)

geolitedb="GeoLite2-City.mmdb"
try:
	reader = maxminddb.Reader(geolitedb)
except:
	logger.error("File {} not found".format(geolitedb))
	sys.exit()

conn=get_connection()

if (args.schemas!=None):
	schemas=args.schemas.split(",")
else:
	schemas=get_coursera_schema_list()

if (args.clean):
	query="DROP TABLE IF EXISTS coursera_geolocate"
	try:
		conn.execute(query)
	except:
		pass

query="""CREATE TABLE IF NOT EXISTS `coursera_geolocate` (
  `ip` varchar(15) DEFAULT NULL,
  `continent` varchar(32) DEFAULT NULL,
  `country` varchar(64) DEFAULT NULL,
  `country_iso` varchar(6) DEFAULT NULL,
  `latitude` float DEFAULT NULL,
  `longitude` float DEFAULT NULL,
  `city` varchar(128) CHARACTER SET utf32 DEFAULT NULL,
  `subdivisions_iso` varchar(6) DEFAULT NULL,
  `subdivisions` varchar(128) CHARACTER SET utf32 DEFAULT NULL,
  `postal` varchar(32) DEFAULT NULL,
  `time_zone` varchar(32) DEFAULT NULL,
  PRIMARY KEY (`ip`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;"""

try:
	conn.execute(query)
except:
	pass
	
metadata = MetaData()
metadata.reflect(bind=conn)
tbl_coursera_geolocate=metadata.tables["coursera_geolocate"]

query=args.sql
#todo: should get the list of IPs already setup in the db so we don't have to bother
#looking them up again
ip_set=set()
for schema in schemas:
	try:
		schema_conn=get_connection(schema)
		results=schema_conn.execute(query)
		for row in results:
			#sometimes multiple IPs
			ip=row[0]
			if len(ip.split(",")) > 1:
				map(ip_set.add,ip.split(","))
			else:
				ip_set.add(ip)
	except Exception, e:
		logger.warn("Error accessing schem {} with exception {}".format(schema, e))
		continue

for ip in ip_set:
	try:
		entry_dict=reader.get(ip.strip())
		output_dict={"ip":ip.strip()}
		output_dict["continent"]=__get_nested_item(["continent","names","en"], entry_dict)
		output_dict["country"]=__get_nested_item(["country","names","en"], entry_dict)
		output_dict["country_iso"]=__get_nested_item(["country","iso_code"], entry_dict)
		output_dict["latitude"]=__get_nested_item(["location","latitude"], entry_dict)
		output_dict["longitude"]=__get_nested_item(["location","longitude"], entry_dict)
		output_dict["city"]=__get_nested_item(["city","names","en"], entry_dict)
		output_dict["subdivisions_iso"]=__get_nested_item(["subdivisions","iso_code"], entry_dict)
		output_dict["subdivisions"]=__get_nested_item(["subdivisions","names","en"], entry_dict)
		output_dict["postal"]=__get_nested_item(["postal","code"], entry_dict)
		output_dict["time_zone"]=__get_nested_item(["location","time_zone"], entry_dict)
		conn.execute( tbl_coursera_geolocate.insert().values(output_dict) )
	except IntegrityError, ie:
		#supress duplicate key errors in MySQL
		if str(ie.orig).startswith("1062: Duplicate"):
			continue
		else:
			logger.warn("Error entering data for ip {} {}".format(ip,ie))
	except Exception, e:
		logger.warn("Error entering data for ip {} {}".format(ip,e))

reader.close()
