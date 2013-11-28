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

import re

def __strip_prefix(name):
  name = re.sub(r'\A(mr|ms|mrs|dr|md|prof|miss|master) ','',name).strip()
  name = re.sub(r'\A,','',name).strip()
  return name

def __strip_suffix(name):
  name = re.sub(r' (jr|sr|esq|dr|md|dds|mba|phd)\Z','',name).strip()
  name = re.sub(r',\Z','',name).strip()
  return name

def clean(name):
  """Returns a tuple, (firstname,middlename,lastname,confidence) with best 
  guesses for each name value and an indication of how confident the cleaning
  process was.
  """
  name = str(name) #since pandas might pass in numbers as ints
  name = name.lower()
  name = re.sub(r'\.', '', name).strip()       # remove .'s
  name = re.sub(r'[\s]+', ' ', name).strip()   # remove multiple \s

  while name != __strip_prefix(name):
    # remove prefixes
    name = __strip_prefix(name)
  while name != __strip_suffix(name):
    # remove suffixes
    name = __strip_suffix(name)

  # split name on surname prefix
  surname_prefix_found = False
  surname_prefixes = ['van','von','de','da','dos','del','la','el','al','der','bin','di','ben','abu','du','dal','della','mac','haj','ter','neder','ibn','ab','nic','ek','lund','beck','oz','berg','papa','hadj','bar','skog','bjorn','degli','holm']
  for surname_prefix in surname_prefixes:
    if re.match(r'\A.*\s('+surname_prefix+')\s.*\Z', name):
      name_re = re.search(r'\A(.*)\s(('+surname_prefix+')\s.*)\Z', name)
      values = (name_re.group(1),'',name_re.group(2), 1)
      surname_prefix_found = True
      break

  if surname_prefix_found == False:
    if re.match(r'\A[^\s]+\Z', name):
      # first (no \s)
      values = (name, None, None, 1)
    elif re.match(r'\A[^\s]+[\s][^\s]+\Z', name):
      # first + last (split on \s)
      name_re = re.search(r'\A([^\s]+)[\s]([^\s]+)\Z', name)
      values = (name_re.group(1), None, name_re.group(2), 1)
    elif re.match(r'\A[^\s]+[\s][^\s][\s][^\s]+\Z', name):
      # first + mi + last (split on mi)
      name_re = re.search(r'\A([^\s]+)[\s]([^\s])[\s]([^\s]+)\Z', name)
      values = (name_re.group(1), name_re.group(2), name_re.group(3), 1)
    elif re.match(r'\A[^\s]+[\s][^\s]+[\s][^\s]+\Z', name):
      # first + middle + last (split on middle)
      name_re = re.search(r'\A([^\s]+)[\s]([^\s]+)[\s]([^\s]+)\Z', name)
      values = (name_re.group(1), name_re.group(2), name_re.group(3), 1)
    else:
      # other
      values = (name, None, None, 0)
  return values
