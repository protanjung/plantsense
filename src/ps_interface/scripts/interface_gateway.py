#!/usr/bin/python

import rospy


class InterfaceGateway():
    def __init__(self):
        pass


if __name__ == "__main__":
    rospy.init_node("interface_gateway")
    interface_gateway = InterfaceGateway()
    rospy.spin()
