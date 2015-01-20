# Copyright (C) 2013  The Regents of the University of Michigan
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
from util.config import *


class InvalidCourseraUrlException(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


logger = get_logger("url_parser.py")


def url_to_tuple(url):
    """Takes a coursera resource url and returns a tuple
  (session_id,path,resource,parameters), e.g. 
  'https://class.coursera.org/sna-002/forum/list?forum_id=2' 
  would be returned as ("sna-002","forum","list","forum_id=2").  
  Path parameters do not include leading or trailing slashes. 
  Returns None of the function is not a coursera url, and 
  throws a InvalidCourseUrlException if the URL cannot be 
  decomposed."""

    if url[0] == '"':
        url = url[1:]
        if url[-1] == '"':
            url = url[:-1]
    url = url.strip()

    try:
        url_re = re.search(r'\A(http|https)://(accounts|class|www).coursera.org/(.*)', url)
        decompose = url_re.group(3)
        decompose = decompose.replace('//', '/')
        remaining = decompose.count('/')

        try:
            session_id = None
            path = None
            end = None
            resource = None
            parameters = None

            if remaining >= 2:
                decompose_re = re.search(r'\A([^/]+)/([^/]+)/(.*)\Z', decompose)
                session_id = decompose_re.group(1)
                path = decompose_re.group(2)
                end = decompose_re.group(3)
            elif remaining == 1:
                decompose_re = re.search(r'\A([^/]+)/(.*)\Z', decompose)
                session_id = decompose_re.group(1)
                path = decompose_re.group(2)
                # todo: path might have a url parameter list in it, e.g. if it were introfinance-002/something?x=y
            else:
                session_id = decompose

            if end is not None:
                if end.find('?') != -1:
                    # parameters
                    resource, parameters = end.split('?', 1)
                elif end.find('#') != -1:
                    resource, parameters = end.split('#', 1)
            else:
                # no parameters
                resource = end
                parameters = None
            return session_id, path, resource, parameters

        except Exception as e:
            # return InvalidCourseUrlException if URL cannot be decomposed
            raise InvalidCourseraUrlException(e.value)

    except Exception as e:
        # not a Coursera URL
        raise InvalidCourseraUrlException("Invalid URL {}".format(url))