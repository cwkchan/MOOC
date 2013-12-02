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

def get_params(params):
  param_list = {}
  for param in params.split('&amp;'):
    param, val = param.split('=')
    param_list[param] = val
  return param_list

test_cases = []
test_cases.append('https://class.coursera.org/sna-002/lecture/view?lecture_id=2')
test_cases.append('https://class.coursera.org/sna-002/lecture/view?lecture_id=75&amp;preview=1')
test_cases.append('https://class.coursera.org/sna-002/forum/thread?thread_id=976')
test_cases.append('https://class.coursera.org/sna-002/forum/list?forum_id=2')

for url in test_cases:
  url_re = re.search(r'\Ahttps://class.coursera.org/([^/]+)/([^/]+)/([^/]+)\?([^/]+\Z)', url)
  session_id = url_re.group(1)
  item_type = url_re.group(2)
  action = url_re.group(3)
  params = get_params(url_re.group(4))
  if item_type == 'lecture':
    print item_type+': '+action+' (lecture_id='+params['lecture_id']+')'
  elif item_type == 'forum':
    if action == 'thread':
      print item_type+': '+action+' (thread_id='+params['thread_id']+')'
    elif action == 'list':
      print item_type+': '+action+' (forum_id='+params['forum_id']+')'
