#!/usr/bin/python3

import rospy
import mysql.connector
import threading
from ps_common.srv import *


# =====Parameter
mysql_host = None
mysql_port = None
mysql_database = None
mysql_table_data = None
mysql_table_meta = None
mysql_username = None
mysql_password = None
# =====ServiceServer
srv_mysql_insert = None
# =====Mutex
mutex_mysql = threading.Lock()

# -----MySQL
mysql_connector = None
mysql_cursor = None


# --------------------------------------
# ======================================


def cllbck_srv_mysql_update(req):
    global mysql_database
    global mysql_table_data

    global mysql_connector
    global mysql_cursor

    global mutex_mysql

    sql = 'INSERT INTO `{}`.`{}` (name,value,quality,`timestamp`) VALUES (%s,%s,%s,%s)'.format(mysql_database, mysql_table_data)

    try:
        # Locking the mutex.
        mutex_mysql.acquire()

        # Checking if the connection to the database is successful. If it is not,
        # it will log an error and return -1.
        if mysql_connect() == -1:
            rospy.logerr('MySQL connection failed')
            return -1

        # Inserting the data into the database.
        mysql_cursor.execute(sql, (req.name, req.value, req.quality, req.timestamp))
        mysql_connector.commit()

        # Checking if the disconnection was successful. If it is not,
        # it will log an error and return -1.
        if mysql_disconnect() == -1:
            rospy.logerr('MySQL disconnection failed')
            return -1

        # Unlocking the mutex.
        mutex_mysql.release()

    except mysql.connector.Error as err:
        rospy.logerr('MySQL error: {}'.format(err))
        return -1

    return mysql_insertResponse()


# --------------------------------------
# ======================================


def mysql_connect():
    """
    It connects to the MySQL database
    :return: the value of the variable "err"
    """
    global mysql_host
    global mysql_port
    global mysql_username
    global mysql_password

    global mysql_connector
    global mysql_cursor

    try:
        mysql_connector = mysql.connector.connect(
            host=mysql_host,
            port=mysql_port,
            user=mysql_username,
            password=mysql_password
        )
        mysql_cursor = mysql_connector.cursor()
        return 0
    except mysql.connector.Error as err:
        rospy.logerr('MySQL error: {}'.format(err))
        return -1


def mysql_disconnect():
    """
    It closes the connection to the MySQL database
    :return: The return value is the number of rows affected by the last query.
    """
    global mysql_connector
    global mysql_cursor

    try:
        mysql_cursor.close()
        mysql_connector.close()
        return 0
    except mysql.connector.Error as err:
        rospy.logerr('MySQL error: {}'.format(err))
        return -1

# --------------------------------------
# ======================================


if __name__ == '__main__':
    rospy.init_node('interface_mysql')

    # =====Parameter
    mysql_host = rospy.get_param('mysql/host')
    mysql_port = rospy.get_param('mysql/port')
    mysql_database = rospy.get_param('mysql/database')
    mysql_table_data = rospy.get_param('mysql/table_data')
    mysql_table_meta = rospy.get_param('mysql/table_meta')
    mysql_username = rospy.get_param('mysql/username')
    mysql_password = rospy.get_param('mysql/password')
    # =====ServiceServer
    srv_mysql_insert = rospy.Service('mysql/insert', mysql_insert, cllbck_srv_mysql_update)

    rospy.spin()
