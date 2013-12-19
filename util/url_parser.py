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

def url_to_tuple(url):
  """Takes a coursera resource url and returns a tuple 
  (course_id,path,resource,parameters), e.g. 
  'https://class.coursera.org/sna-002/forum/list?forum_id=2' 
  would be returned as ("sna-002","forum","list","forum_id=2").  
  Path parameters do not include leading or trailing slashes. 
  Returns None of the function is not a coursera url, and 
  throws a InvalidCourseUrlException if the URL cannot be 
  decomposed."""

  try:
    try:
      url_re = re.search(r'\Ahttps://class.coursera.org/([^/]+)/([^/]+)/(.*\Z)', url)
      course_id = url_re.group(1)
      path = url_re.group(2)
      end = url_re.group(3)
    except:
      # not a Coursera URL
      return None
      
    if end.find('?') != -1:
      # parameters
      resource, parameters = end.split('?')
    elif end.find('#') != -1:
      resource, parameters = end.split('#')
    else:
      # no parameters
      resource = end
      parameters = None

    return (course_id, path, resource, parameters)
    
  except Exception, e:
    # return InvalidCourseUrlException if URL cannot be decomposed
    return 'InvalidCourseUrlException'

'''
test_cases = []
test_cases.append('https://class.coursera.org/sna-002/lecture/view?lecture_id=2')
test_cases.append('https://class.coursera.org/sna-002/lecture/view?lecture_id=75&amp;preview=1')
test_cases.append('https://class.coursera.org/sna-002/lecture/9')
test_cases.append('https://class.coursera.org/sna-002/forum/thread?thread_id=976')
test_cases.append('https://class.coursera.org/sna-002/forum/list?forum_id=2')
test_cases.append('https://class.coursera.org/sna-002/forum/index#blahblah')
test_cases.append('https://class.coursera.org/sna-002/class/index')
test_cases.append('https://class.coursera.org/sna-002/class/preferences')
test_cases.append('https://class.coursera.org/sna-002/wiki/view?id=blah')
test_cases.append('http://www.google.com/')
test_cases.append('http://www.coursera.org/')
test_cases.append('https://class.coursera.org/sna-002/class/preferences?preferences?')
test_cases.append('https://class.coursera.org/sna-002/forum/index#forum-threads-all-0-state-page_num=39')

for case in test_cases:
  print url_to_tuple(case)
'''
