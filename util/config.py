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
import Queue
import threading
import time
import mysql.connector
import ConfigParser
from sqlalchemy import *
import logging
import os


def get_properties():
    """Returns the list of properties as a dict of key/value pairs in the file config.properties."""
    cf = ConfigParser.ConfigParser()
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

    query = "SELECT id FROM coursera_index WHERE start IS NOT NULL;"
    conn = get_connection()
    results = conn.execute(query)
    schemas = []
    for row in results:
        schemas.append(row["id"].encode('ascii', 'ignore'))
    return schemas


def convert_sessionid_to_id(session_id):
    """Converts a session id in the form of 'introfinance-001' to a primary key such as 2.  Simple wrapper to select
    from the uselab_mooc.coursera_index table"""
    conn=get_connection()
    rs = conn.execute("select id from coursera_index where session_id like '{}'".format(session_id))
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


class ThreadedDBQueue(threading.Thread):
    """
    Manages a queue for a given db connection, and table in separate thread.
    """

    def __init__(self, queue, connection, table, batch_size=1000, log_to_console=False, hard_exit_on_failure=False):
        """
        queue should be of type Queue.Queue(), connection of type util.config.get_connection() (an SQLAlchemy engine),
        and table should be an SQLAlchemy table metadata object (e.g. metadata.tables["coursera_clickstream"]).
        """
        threading.Thread.__init__(self)
        self.queue = queue
        self.connection = connection
        self.table = table
        self.close = False
        self.batch_size = batch_size
        self.log_to_console = log_to_console
        self.hard_exit_on_failure = hard_exit_on_failure

    def stop(self):
        """
        Indicates that the ThreadedDBQueue should shutdown after it has emptied its queue.
        """
        self.close = True


    def run(self):
        """
        Start the ThreadedDBQueue running, inserting up to batch_size items at once.
        """
        while (not self.close) or self.queue.qsize() > 0:
            items = []
            for item_num in range(0, self.batch_size):
                try:
                    items.append(self.queue.get_nowait())
                except Queue.Empty:
                    continue
            if self.log_to_console:
                print("{} items being put in db for table {}, {} remain in queue.".format(len(items), self.table,
                                                                                          self.queue.qsize()))
            try:
                self.connection.execute(self.table.insert().values(items))
            except Exception, e:
                if self.log_to_console:
                    print("Exception {}".format(e))
                if self.hard_exit_on_failure:
                    os._exit(-1)