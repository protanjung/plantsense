#!/usr/bin/python3

import rospy
from ps_ros_lib.help_log import help_log
from ps_interface.msg import fuel_input, fuels_input
from pulp import LpMinimize, LpProblem, LpStatus, lpSum, LpVariable

class optimizer:
    def __init__(self):
        # =====Subscriber
        self.sub_fuels_input = rospy.Subscriber('fuels_input', fuels_input, self.cllbck_sub_fuels_input, queue_size=1)
        # =====Help
        self.help_log = help_log()

        self.is_initialized = False

        if (self.optimizer_init() == -1):
            rospy.signal_shutdown("")

    # --------------------------------------------------------------------------
    # ==========================================================================

    def cllbck_sub_fuels_input(self, msg):
        num_of_fuel = len(msg.fuels_input)

        # =====
        fuel_needs = 131194
        # =====

        min_volumes = []
        for i in range(num_of_fuel):
            min_volumes.append(msg.fuels_input[i].min_volume)
        max_volumes = []
        for i in range(num_of_fuel):
            max_volumes.append(msg.fuels_input[i].max_volume)
        prices = []
        for i in range(num_of_fuel):
            prices.append(msg.fuels_input[i].price)
        # -----
        fuels = []
        for i in range(num_of_fuel):
            fuels.append(LpVariable(msg.fuels_input[i].name, min_volumes[i], max_volumes[i]))
        # -----
        prob = LpProblem('PlantSense Optimizer', LpMinimize)
        prob += lpSum([prices[i] * fuels[i] for i in range(num_of_fuel)])
        prob += (lpSum([fuels[i] for i in range(num_of_fuel)]) == fuel_needs, 'fuel_needs')
        # -----
        status = prob.solve()

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
