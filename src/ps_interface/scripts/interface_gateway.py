#!/usr/bin/python3

import rospy
from ps_ros_lib.help_log import help_log

class interface_gateway:
    def __init__(self):
        # =====Parameter
        self.gw_reset = rospy.get_param('gw/reset', 'http://localhost:5000/reset')
        self.gw_data = rospy.get_param('gw/data', 'http://localhost:5000/data')
        self.gw_register_group = rospy.get_param('gw/register_group', 'http://localhost:5000/register_group')
        self.gw_register_period = rospy.get_param('gw/register_period', 'http://localhost:5000/register_period')
        # =====Timer
        self.tim_50hz = rospy.Timer(rospy.Duration(0.02), self.cllbck_tim_50hz)
        self.tim_100hz = rospy.Timer(rospy.Duration(0.01), self.cllbck_tim_100hz)
        # =====Help
        self._log = help_log()

        if (self.gateway_init() == -1):
            rospy.signal_shutdown("")

    # --------------------------------------------------------------------------
    # ==========================================================================

    def cllbck_tim_50hz(self, event):
        pass

    def cllbck_tim_100hz(self, event):
        pass

    # --------------------------------------------------------------------------
    # ==========================================================================

    def gateway_init(self):
        rospy.sleep(2)
        self._log.info("GW Reset           : " + self.gw_reset)
        self._log.info("GW Data            : " + self.gw_data)
        self._log.info("GW Register Group  : " + self.gw_register_group)
        self._log.info("GW Register Period : " + self.gw_register_period)

        return 0

    def gateway_routine(self):
        return 0


if __name__ == '__main__':
    rospy.init_node('interface_gateway')
    interface_gateway = interface_gateway()
    rospy.spin()
