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

import multiprocessing
import queue
import configparser as configparser
from sqlalchemy import *
import logging
import os
from ctypes import c_bool
import csv
import io

def get_properties():
    """Returns the list of properties as a dict of key/value pairs in the file config.properties."""
    cf = configparser.ConfigParser()
    cf.read("config.properties")
    properties = {}
    for section in cf.sections():
        for item in cf.items(section):
            properties[item[0]] = item[1]
    return properties


def get_connection(schema=None):
    """Returns an sqlalchemy connection object, optionally connecting to a particular schema of interest.  If no schema
    is used, the one marked as the index in the configuration file will be used. """
    config = get_properties()
    if (schema == None):
        return create_engine(config["engine"] + config["schema"])
    return create_engine(config["engine"] + schema)


def get_coursera_schema_list():
    """Returns the list of courses, as db schemas, that should be processed. This list comes from either the
    configuration file in the schemas section or, if that does not exist, it comes from the coursera_index table."""
    config = get_properties()
    if ("schemas" in config.keys()):
        return config["schemas"].split(",")

    query = "SELECT session_id FROM coursera_index WHERE start IS NOT NULL;"
    conn = get_connection()
    results = conn.execute(query)
    schemas = []
    for row in results:
        schemas.append(row["session_id"])
    return schemas


def convert_sessionid_to_id(session_id):
    """Converts a session id in the form of 'introfinance-001' to a primary key such as 2.  Simple wrapper to select
    from the uselab_mooc.coursera_index table"""
    conn = get_connection()
    rs = conn.execute("SELECT admin_id FROM coursera_index WHERE session_id LIKE '{}'".format(session_id))
    id = int(rs.fetchone()[0])
    return id


def get_logger(name, verbose=False):
    """Returns a logger with the given name at either the debug or info level"""
    logger = logging.getLogger(name)
    if verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    return logger


def filename_to_schema(fn):
    """Attempts to turn a filename in the form path\to\course.csv into the value course"""
    return os.path.basename(fn).split(".")[0]


class ThreadedCSVQueue(multiprocessing.Process):
    """
    Manages a queue for a given file so that it can be loaded later with MySQL's LOAD INFILE
    """
    def __init__(self, queue, connection, table, batch_size=1, log_to_console=False, hard_exit_on_failure=True,
                 write_header=False):
        """
        queue should be of type Queue.Queue(), connection of type file.
        """
        multiprocessing.Process.__init__(self)
        self.queue = queue
        self.connection = connection
        self.table = table
        self.close = multiprocessing.Value(c_bool, False)
        self.batch_size = batch_size
        self.log_to_console = log_to_console
        self.hard_exit_on_failure = hard_exit_on_failure
        self.failures = []
        self.file=connection

        # start csv file up
        self.columns=[]
        for col in table.columns:
            self.columns.append(str(col).replace("{}.".format(table.name),"")) # file is in colname, metadata in table.colname

        if write_header:
            self.writer = csv.DictWriter(connection, fieldnames=self.columns, delimiter='\t', doublequote=True, escapechar='\\',
                                     skipinitialspace=True)
            self.writer.writeheader()

    def stop(self):
        """
        Indicates that the ThreadedCSVQueue should shutdown after it has emptied its queue.
        """
        self.close.value = True

    def __insert_values(self, items):
        """
        Tries to append items into file.  Returns True if success else False, and exits the thread if
        hard_exit_on_failure is set.
        """
        try:
            for item in items:
                out_string=io.StringIO('')
                wrtr = csv.DictWriter(out_string, fieldnames=self.columns, delimiter='\t', doublequote=True, escapechar='\\', skipinitialspace=True)
                #it seems lame to have to do this, why can't this be done autmatically by the writer?
                for k in item.keys():
                    if item[k]==None:
                        item[k]='NULL'
                missing=set(self.columns)-set(item.keys())
                for i in missing:
                    item[i]='NULL'
                wrtr.writerow(item)

                self.file.write(out_string.getvalue().replace("NULL",r'\N')) #supposedly this instructs mysql to insert a null
            return True
        except Exception as e:
            print(e)
            if self.batch_size>1:
                print("There was an exception in batch insert, will report on individual values.")
            else:
                if self.log_to_console:
                    print("Exception {}".format(e))
                    print("Item: {}".format(items))
                if self.hard_exit_on_failure:
                    os._exit(-1)
            return False

    def run(self):
        """
        Start the ThreadedCSVQueue running, inserting up to batch_size items at once.
        """
        if self.log_to_console:
            print("Starting CSV Queue.")
        while self.queue.qsize() > 0 or self.close.value is False:
            items = []
            for item_num in range(0, self.batch_size):
                try:
                    items.append(self.queue.get_nowait())
                except queue.Empty:
                    continue
            if self.log_to_console:
                print("{} items being put in db for table {}, {} remain in queue.".format(len(items), self.table,
                                                                                          self.queue.qsize()))
            # try and add all the items in the queue and, if the batch fails, then try and add them one by one
            if not self.__insert_values(items):
                for item in items:
                    if not self.__insert_values(item):
                        self.failures.append(item)

class ThreadedDBQueue(multiprocessing.Process):
    """
    Manages a queue for a given db connection, and table in separate thread.
    """

    def __init__(self, queue, connection, table, batch_size=1, log_to_console=False, hard_exit_on_failure=True):
        """
        queue should be of type Queue.Queue(), connection of type util.config.get_connection() (an SQLAlchemy engine),
        and table should be an SQLAlchemy table metadata object (e.g. metadata.tables["coursera_clickstream"]).
        """
        multiprocessing.Process.__init__(self)
        self.queue = queue
        self.connection = connection
        self.table = table
        self.close = multiprocessing.Value(c_bool, False)
        self.batch_size = batch_size
        self.log_to_console = log_to_console
        self.hard_exit_on_failure = hard_exit_on_failure
        self.failures = []

    def stop(self):
        """
        Indicates that the ThreadedDBQueue should shutdown after it has emptied its queue.
        """
        self.close.value = True

    def __insert_values(self, items):
        """
        Tries to insert multiple items into this threads table.  Returns True if success else False, and exits the
        thread if hard_exit_on_failure is set.
        """
        try:
            self.connection.execute(self.table.insert().values(items))
            return True
        except Exception as e:
            if self.batch_size>1:
                print("There was an exception in batch insert, will report on individual values.")
            else:
                if self.log_to_console:
                    print("Exception {}".format(e))
                    print("Item: {}".format(items))
                if self.hard_exit_on_failure:
                    os._exit(-1)
            return False

    def run(self):
        """
        Start the ThreadedDBQueue running, inserting up to batch_size items at once.
        """
        if self.log_to_console:
            print("Starting DB Queue.")
        while self.queue.qsize() > 0 or self.close.value is False:
            items = []
            for item_num in range(0, self.batch_size):
                try:
                    items.append(self.queue.get_nowait())
                except queue.Empty:
                    continue
            if self.log_to_console:
                print("{} items being put in db for table {}, {} remain in queue.".format(len(items), self.table,
                                                                                          self.queue.qsize()))
            # try and add all the items in the queue and, if the batch fails, then try and add them one by one
            if not self.__insert_values(items):
                for item in items:
                    if not self.__insert_values(item):
                        self.failures.append(item)