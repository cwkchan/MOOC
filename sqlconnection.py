import mysql.connector
import ConfigParser
import types

__sqlconnection_connections = {}


def close(self):
    """A way to override the close method"""
    del(__sqlconnection_connections[self.__schema_name])
    self.close_connection()


def get_connection(db, refresh=False):
    global __sqlconnection_conn
    if (db in __sqlconnection_connections) and (not refresh):
        return __sqlconnection_connections[db]
    else:
        # read database configuration
        cf = ConfigParser.ConfigParser()
        cf.read("db.properties")
        cf.items("connection")
        __sqlconnection_conn = mysql.connector.connect(user=cf.get("connection", "user"), password=cf.get(
            "connection", "password"), host=cf.get("connection", "host"), database=db)
        __sqlconnection_connections[db] = __sqlconnection_conn
        __sqlconnection_conn.__schema_name = db  # reverse pointer
        __sqlconnection_conn.close_connection = __sqlconnection_conn.close
        # When adding a new function we must bind it as a method to the object
        __sqlconnection_conn.close = types.MethodType(
            close, __sqlconnection_conn)
        return __sqlconnection_conn
