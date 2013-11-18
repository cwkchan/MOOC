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
from sqlconnection import *
import re

def strip_prefix(name):
  name = re.sub(r'\A(mr|ms|mrs|dr|md|prof|miss|master) ','',name).strip()
  name = re.sub(r'\A,','',name).strip()
  return name

def strip_suffix(name):
  name = re.sub(r' (jr|sr|esq|dr|md|dds|mba|phd)\Z','',name).strip()
  name = re.sub(r',\Z','',name).strip()
  return name

parser = argparse.ArgumentParser(description='Copy pii data from csv to SQL database.')
parser.add_argument('--clean', action='store_true', help='Whether to drop tables in the database or not')
args = parser.parse_args()

uselabconn = get_connection('uselab_mooc')
uselabcursor = uselabconn.cursor()

if (args.clean):
  sql_drop_coursera_pii_cleaned = '''DROP TABLE IF EXISTS `coursera_pii_cleaned`;'''
  uselabcursor.execute(sql_drop_coursera_pii_cleaned)

sql_create_coursera_pii_cleaned = '''
  CREATE TABLE IF NOT EXISTS `coursera_pii_cleaned` (
  `user_id` INT NOT NULL,
  `session_id` VARCHAR(255) NOT NULL,
  `first_name` VARCHAR(255) CHARACTER SET utf32 DEFAULT NULL,
  `middle_name` VARCHAR(255) CHARACTER SET utf32 DEFAULT NULL,
  `last_name` VARCHAR(255) CHARACTER SET utf32 DEFAULT NULL,
  `confidence` TINYINT DEFAULT NULL,
  `email` VARCHAR(255) DEFAULT NULL,
  PRIMARY KEY (`user_id`, `session_id`)
  ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
  '''
uselabcursor.execute(sql_create_coursera_pii_cleaned)

sql_select_all_coursera_pii = '''SELECT * from `coursera_pii`'''
uselabcursor.execute(sql_select_all_coursera_pii)

#counts = {}
#counts['total'] = 0
#counts['suffix'] = 0
#counts['prefix'] = 0
#counts['surname_prefix'] = 0
#counts['first'] = 0
#counts['first_last'] = 0
#counts['first_mi_last'] = 0
#counts['first_middle_last'] = 0
#counts['other'] = 0

coursera_pii_cleaned = []
for row in uselabcursor:
  coursera_pii_cleaned.append(row)

sql_insert_into_coursera_pii_cleaned = '''INSERT INTO coursera_pii_cleaned (`user_id`, `session_id`, `first_name`, `middle_name`, `last_name`, `confidence`, `email`) VALUES (%s,%s,%s,%s,%s,%s,%s);'''
for row in coursera_pii_cleaned:
  name = row[2].lower()
  name = re.sub(r'\.', '', name).strip()       # remove .'s
  name = re.sub(r'[\s]+', ' ', name).strip()   # remove multiple \s

  while name != strip_prefix(name):
    # remove prefixes
    name = strip_prefix(name)
    #counts['prefix'] += 1
  while name != strip_suffix(name):
    # remove suffixes
    name = strip_suffix(name)
    #counts['suffix'] += 1

  # split name on surname prefix
  surname_prefix_found = False
  surname_prefixes = ['van','von','de','da','dos','del','la','el','al','der','bin','di','ben','abu','du','dal','della','mac','haj','ter','neder','ibn','ab','nic','ek','lund','beck','oz','berg','papa','hadj','bar','skog','bjorn','degli','holm']
  for surname_prefix in surname_prefixes:
    if re.match(r'\A.*\s('+surname_prefix+')\s.*\Z', name):
      name_re = re.search(r'\A(.*)\s(('+surname_prefix+')\s.*)\Z', name)
      values = (row[0], row[1], name_re.group(1),'',name_re.group(2), 1, row[3])
      #counts['surname_prefix'] += 1
      surname_prefix_found = True
      break

  if surname_prefix_found == False:
    if re.match(r'\A[^\s]+\Z', name):
      # first (no \s)
      values = (row[0], row[1], name, '', '', 1, row[3])
      #counts['first'] += 1
    elif re.match(r'\A[^\s]+[\s][^\s]+\Z', name):
      # first + last (split on \s)
      name_re = re.search(r'\A([^\s]+)[\s]([^\s]+)\Z', name)
      values = (row[0], row[1], name_re.group(1), '', name_re.group(2), 1, row[3])
      #counts['first_last'] += 1
    elif re.match(r'\A[^\s]+[\s][^\s][\s][^\s]+\Z', name):
      # first + mi + last (split on mi)
      name_re = re.search(r'\A([^\s]+)[\s]([^\s])[\s]([^\s]+)\Z', name)
      values = (row[0], row[1], name_re.group(1), name_re.group(2), name_re.group(3), 1, row[3])
      #counts['first_mi_last'] += 1
    elif re.match(r'\A[^\s]+[\s][^\s]+[\s][^\s]+\Z', name):
      # first + middle + last (split on middle)
      name_re = re.search(r'\A([^\s]+)[\s]([^\s]+)[\s]([^\s]+)\Z', name)
      values = (row[0], row[1], name_re.group(1), name_re.group(2), name_re.group(3), 1, row[3])
      #counts['first_middle_last'] += 1
    else:
      # other
      values = (row[0], row[1], name, '', '', 0, row[3])
      #counts['other'] += 1
  
  uselabcursor.execute(sql_insert_into_coursera_pii_cleaned, values)
  uselabconn.commit()
  #counts['total'] += 1

#print 'Total entries: '+str(counts['total'])
#print '  [prefix]: '+str(counts['prefix'])+' prefixes, ('+str(100*counts['prefix']/counts['total'])+'%)'
#print '  [suffix]: '+str(counts['suffix'])+' suffixes, ('+str(100*counts['suffix']/counts['total'])+'%)'
#print '  [surname prefix]: '+str(counts['surname_prefix'])+' surname_prefixes, ('+str(100*counts['surname_prefix']/counts['total'])+'%)'
#print '  [first]: '+str(counts['first'])+' matches ('+str(100*counts['first']/counts['total'])+'%)'
#print '  [first last]: '+str(counts['first_last'])+' matches ('+str(100*counts['first_last']/counts['total'])+'%)'
#print '  [first mi last]: '+str(counts['first_mi_last'])+' matches ('+str(100*counts['first_mi_last']/counts['total'])+'%)'
#print '  [first middle last]: '+str(counts['first_middle_last'])+' matches ('+str(100*counts['first_middle_last']/counts['total'])+'%)'
#print '  [other]: '+str(counts['other'])+' matches ('+str(100*counts['other']/counts['total'])+'%)'

uselabconn.close()
