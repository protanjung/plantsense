#!/usr/bin/python3

import rospy
from ps_ros_lib.help_log import help_log
from ps_interface.msg import opc, opcs
import json
import requests


class interface_gateway:
    def __init__(self):
        # =====Parameter
        self.gw_reset = rospy.get_param('gw/reset', 'http://localhost:5000/reset')
        self.gw_data = rospy.get_param('gw/data', 'http://localhost:5000/data')
        self.gw_register_group = rospy.get_param('gw/register_group', 'http://localhost:5000/register_group')
        self.gw_register_period = rospy.get_param('gw/register_period', 'http://localhost:5000/register_period')
        # =====Timer
        self.tim_1hz = rospy.Timer(rospy.Duration(1), self.cllbck_tim_1hz)
        self.tim_50hz = rospy.Timer(rospy.Duration(0.02), self.cllbck_tim_50hz)
        # =====Publisher
        self.pub_opcs = rospy.Publisher('opcs', opcs, queue_size=0)
        # =====Help
        self._log = help_log()

        if (self.gateway_init() == -1):
            rospy.signal_shutdown("")

    # --------------------------------------------------------------------------
    # ==========================================================================

    def cllbck_tim_1hz(self, event):
        if (self.gateway_routine() == -1):
            rospy.signal_shutdown("")

    def cllbck_tim_50hz(self, event):
        pass

    # --------------------------------------------------------------------------
    # ==========================================================================

    def gateway_init(self):
        # Printing the parameters.
        rospy.sleep(2)
        self._log.info("Gateway Reset: " + self.gw_reset)
        self._log.info("Gateway Data: " + self.gw_data)
        self._log.info("Gateway Register Group: " + self.gw_register_group)
        self._log.info("Gateway Register Period: " + self.gw_register_period)

        return 0

    def gateway_routine(self):
        # Sending a GET request to the URL stored in the parameter
        # `gw_data`. Received data is stored in the variable `response`.
        response = requests.request("GET", self.gw_data)

        # Checking if the response is 200, if it is,
        # it is loading the data into a dictionary.
        response_dict = {}
        if (response.status_code == 200):
            response_dict = json.loads(response.text)
        else:
            self._log.error("Gateway Data: " + str(response.status_code) + " " + response.text)
            return 0

        # Creating a message of type `opcs` and filling it
        # with the data from the dictionary then publishing it.
        msg_opcs = opcs()
        for key, value in response_dict["opc_data_all_server"].items():
            msg_opc = opc()
            msg_opc.name = value["name"]
            msg_opc.value = value["value"]
            msg_opc.timestamp = value["timestamp"]
            msg_opcs.opcs.append(msg_opc)
        self.pub_opcs.publish(msg_opcs)

        return 0


if __name__ == '__main__':
    rospy.init_node('interface_gateway')
    interface_gateway()
    rospy.spin()
