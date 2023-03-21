#!/usr/bin/python3

import rospy
from ps_ros_lib.help_log import help_log
from ps_interface.msg import opc, opcs
import mysql.connector

class interface_database:
    def __init__(self):
        # =====Parameter
        self.db_host = rospy.get_param('db/host', 'localhost')
        self.db_port = rospy.get_param('db/port', 3306)
        self.db_database = rospy.get_param('db/database', 'ps_database')
        self.db_user = rospy.get_param('db/user', 'ps_user')
        self.db_password = rospy.get_param('db/password', 'ps_password')
        # =====Timer
        self.tim_1hz = rospy.Timer(rospy.Duration(1), self.cllbck_tim_1hz)
        self.tim_50hz = rospy.Timer(rospy.Duration(0.02), self.cllbck_tim_50hz)
        # =====Subscriber
        self.sub_opcs = rospy.Subscriber('opcs', opcs, self.cllbck_sub_opcs)
        # =====Help
        self._log = help_log()

        self.mydb = None
        self.mycursor = None

        if (self.database_init() == -1):
            rospy.signal_shutdown("")

    # --------------------------------------------------------------------------
    # ==========================================================================

    def cllbck_tim_1hz(self, event):
        if (self.database_routine() == -1):
            rospy.signal_shutdown("")

    def cllbck_tim_50hz(self, event):
        pass

    # --------------------------------------------------------------------------
    # ==========================================================================

    def cllbck_sub_opcs(self, msg):
        for opc in msg.opcs:
            self.database_insert_data(opc.name, opc.value, opc.timestamp)

    # --------------------------------------------------------------------------
    # ==========================================================================

    def database_init(self):
        # Printing the parameters.
        rospy.sleep(2)
        self._log.info("Database Host: " + self.db_host)
        self._log.info("Database Port: " + str(self.db_port))
        self._log.info("Database Database: " + self.db_database)
        self._log.info("Database User: " + self.db_user)
        self._log.info("Database Password: " + self.db_password)

        return 0

    def database_routine(self):
        return 0

    # --------------------------------------------------------------------------
    # ==========================================================================

    def database_connect(self):
        try:
            self.mydb = mysql.connector.connect(
                host=self.db_host,
                port=self.db_port,
                user=self.db_user,
                password=self.db_password
            )
            self.mycursor = self.mydb.cursor()
        except Exception as e:
            self._log.error("Database connect error: " + str(e))
            return -1

        return 0

    def database_close(self):
        try:
            self.mycursor.close()
            self.mydb.close()
        except Exception as e:
            self._log.error("Database close error: " + str(e))
            return -1

        return 0

    def database_insert_data(self, opc_name, opc_value, opc_timestamp):
        sql = "INSERT INTO {}.tbl_data (name,value,`timestamp`) VALUES (%s,%s,%s)".format(self.db_database)

        try:
            if self.database_connect() == -1:
                return -1

            self.mycursor.execute(sql, (opc_name, opc_value, opc_timestamp))
            self.mydb.commit()

            if self.database_close() == -1:
                return -1
        except Exception as e:
            self._log.error("Database insert data error: " + str(e))
            return -1

        return 0


if __name__ == '__main__':
    rospy.init_node('interface_database')
    interface_database()
    rospy.spin()
