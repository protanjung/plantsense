#!/usr/bin/python3

import rospy
from ps_ros_lib.help_log import help_log

class interface_database:
    def __init__(self):
        # =====Parameter
        self.db_host = rospy.get_param('db/host', 'localhost')
        self.db_port = rospy.get_param('db/port', 3306)
        self.db_database = rospy.get_param('db/database', 'ps_database')
        self.db_user = rospy.get_param('db/user', 'ps_user')
        self.db_password = rospy.get_param('db/password', 'ps_password')
        # =====Timer
        self.tim_50hz = rospy.Timer(rospy.Duration(0.02), self.cllbck_tim_50hz)
        self.tim_100hz = rospy.Timer(rospy.Duration(0.01), self.cllbck_tim_100hz)
        # =====Help
        self._log = help_log()

        if (self.database_init() == -1):
            rospy.signal_shutdown("")

    # --------------------------------------------------------------------------
    # ==========================================================================

    def cllbck_tim_50hz(self, event):
        pass

    def cllbck_tim_100hz(self, event):
        pass

    # --------------------------------------------------------------------------
    # ==========================================================================

    def database_init(self):
        rospy.sleep(2)
        self._log.info("DB Host     : " + self.db_host)
        self._log.info("DB Port     : " + str(self.db_port))
        self._log.info("DB Database : " + self.db_database)
        self._log.info("DB User     : " + self.db_user)
        self._log.info("DB Password : " + self.db_password)

        return 0

    def database_routine(self):
        return 0


if __name__ == '__main__':
    rospy.init_node('interface_database')
    interface_database = interface_database()
    rospy.spin()
