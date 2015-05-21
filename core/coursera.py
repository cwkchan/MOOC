# Copyright (C) 2013  The Regents of the University of Michigan
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see [http://www.gnu.org/licenses/].
#

from util.config import *

from . import Base
import os.path as path

from sqlalchemy import Column, Integer, String, DateTime, Boolean

class Course(Base):
    __tablename__ = "coursera_index"

    session_id = Column(String(255, convert_unicode=True), index=True, unique=True)
    admin_id = Column(Integer, unique=True, primary_key=True)
    course = Column(String(255))
    instructor = Column(String(255), nullable=True)
    start = Column(DateTime, nullable=True)
    end = Column(DateTime, nullable=True)
    duration = Column(Integer, nullable=True)
    enrollment = Column(String(255), nullable=True)
    url = Column(String(1024), nullable=True)
    allow_signature = Column(Boolean, nullable=True)

    def __repr__(self):
        return "Course {} has an admin_id of {}, is called {} and starts on {}.".format(self.session_id,
                                                                                                  self.admin_id,
                                                                                                  self.course,
                                                                                                  str(self.start))

    def has_clickstream(self):
        """Determines whether the clickstream files for this course have already been downloaded.

        :return: True if a clickstream file exists in the position get_properties()['clickstream'], False if it doesn't
        """
        return path.isfile("{}/{}_clickstream_export.gz".format(get_properties()['clickstream'], self.session_id))

    def has_sql(self):
        """Determines whether the sql files for this course have already been downloaded.  Note, the courses which have
        a : in the name have it replaced with a - in the filename.  This method takes care of this, but it's unclear
        what happens if the course has a - in the name.

        :return: True if *all* of the sql files exist for this course, False if at least one doesn't
        """
        return path.isfile(
            "{}/{} ({})_SQL_anonymized_forum.sql".format(get_properties()['sql'], self.course.replace(":","-"),
                                                         self.session_id)) and path.isfile(
            "{}/{} ({})_SQL_anonymized_general.sql".format(get_properties()['sql'], self.course.replace(":","-"),
                                                           self.session_id)) and path.isfile(
            "{}/{} ({})_SQL_hash_mapping.sql".format(get_properties()['sql'], self.course.replace(":","-"),
                                                     self.session_id)) and path.isfile(
            "{}/{} ({})_SQL_unanonymizable.sql".format(get_properties()['sql'], self.course.replace(":","-"), self.session_id))

    def has_intent(self):
        """Determines whether the intent files for this course have already been downloaded.

        :return: True if an intent file exists in the position get_properties()['intent'], False if it doesn't
        """
        return path.isfile("{}/{}.csv".format(get_properties()['intent'], str(self.session_id)))

    def has_pii(self):
        """Determines whether the pii files for this course have already been downloaded.

        :return: True if an pii file exists in the position get_properties()['intent'], False if it doesn't
        """
        return path.isfile("{}/{}.csv".format(get_properties()['pii'], str(self.session_id)))

    def has_demographics(self):
        """Determines whether the demographics files for this course have already been downloaded.

        :return: True if a demographics file exists in the position get_properties()['demographics'], False if it doesn't
        """

        return path.isfile("{}/{} ({})_Demographics_individual_responses.csv".format(get_properties()['demographics'], self.course, self.session_id))

    def get_intent_filename(self):
        if self.has_intent():
            return "{}/{}.csv".format(get_properties()['intent'], str(self.session_id))
        else:
            return None
