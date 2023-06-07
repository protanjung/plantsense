#!/usr/bin/python3

import rospy
from ps_ros_lib.help_log import help_log
from ps_interface.msg import db_data, fuel_input, fuels_input
from pulp import LpMinimize, LpProblem, LpStatus, lpSum, LpVariable
from pulp.apis import PULP_CBC_CMD

class optimizer:
    def __init__(self):
        # =====Timer
        self.tim_017hz = rospy.Timer(rospy.Duration(1), self.cllbck_tim_017hz)
        self.tim_1hz = rospy.Timer(rospy.Duration(1), self.cllbck_tim_1hz)
        self.tim_50hz = rospy.Timer(rospy.Duration(0.02), self.cllbck_tim_50hz)
        # =====Subscriber
        self.sub_db_data = rospy.Subscriber('db_data', db_data, self.cllbck_sub_db_data, queue_size=1)
        self.sub_fuels_input = rospy.Subscriber('fuels_input', fuels_input, self.cllbck_sub_fuels_input, queue_size=1)
        # =====Help
        self.help_log = help_log()

        self.db_data = db_data()
        self.fuels_input = fuels_input()
        self.fuel_sfc = 0.0
        self.is_initialized = False

        if (self.optimizer_init() == -1):
            rospy.signal_shutdown("")

    # --------------------------------------------------------------------------
    # ==========================================================================

    def cllbck_tim_017hz(self, event):
        pass

    def cllbck_tim_1hz(self, event):
        pass

    def cllbck_tim_50hz(self, event):
        pass

    # --------------------------------------------------------------------------
    # ==========================================================================

    def cllbck_sub_db_data(self, msg):
        if not self.is_initialized:
            return

        self.db_data = msg

    def cllbck_sub_fuels_input(self, msg):
        if not self.is_initialized:
            return

        self.fuels_input = msg

    # --------------------------------------------------------------------------
    # ==========================================================================

    def optimizer_init(self):
        # Mark initialization
        self.is_initialized = True

        return 0


if __name__ == '__main__':
    rospy.init_node('optimizer')
    optimizer = optimizer()
    rospy.spin()
