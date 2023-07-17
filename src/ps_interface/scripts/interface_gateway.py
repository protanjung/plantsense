#!/usr/bin/python3

import rospy
from ps_interface.msg import opc, opcs
import requests


class InterfaceGateway():
    def __init__(self):
        # =====Parameter
        self.gw_url_data = rospy.get_param("gw/url_data", "http://192.168.1.103:5000/data")
        # =====Timer
        self.tim_2hz = rospy.Timer(rospy.Duration(0.5), self.cllbck_tim_2hz)
        # =====Publisher
        self.pub_opcs = rospy.Publisher("opcs", opcs, queue_size=1)

        if self.interface_gateway_init() == -1:
            rospy.signal_shutdown("")

    # --------------------------------------------------------------------------

    def cllbck_tim_2hz(self, event):
        try:
            response = requests.get(self.gw_url_data, timeout=1)
            if response.status_code != 200:
                # rospy.logerr("Error: %s", response.text)
                return
            response_json = response.json()
            if response_json["status"] != 0:
                # rospy.logerr("Error: %s", response_json["error"])
                return
        except Exception as e:
            # rospy.logerr("Error: %s", e)
            return

        msg_opc = self.data_to_opcs(response_json)
        self.pub_opcs.publish(msg_opc)

    # --------------------------------------------------------------------------

    def interface_gateway_init(self):
        return 0

    # --------------------------------------------------------------------------

    def data_to_opcs(self, input):
        opc_data_all_server = input["opc_data_all_server"]

        _opcs = opcs()
        for tag in opc_data_all_server:
            _opc = opc()
            _opc.name = opc_data_all_server[tag]["name"]
            _opc.value = opc_data_all_server[tag]["value"]
            _opc.timestamp = opc_data_all_server[tag]["timestamp"]
            _opcs.opcs.append(_opc)

        return _opcs


if __name__ == "__main__":
    rospy.init_node("interface_gateway")
    interface_gateway = InterfaceGateway()
    rospy.spin()
