#!/usr/bin/python3

import rospy
from ps_ros_lib.help_log import help_log
from ps_interface.msg import db_data, fuel_input, fuels_input
from std_msgs.msg import String
from pulp import LpMinimize, LpProblem, LpStatus, lpSum, LpVariable
from pulp.apis import PULP_CBC_CMD

class optimizer:
    def __init__(self):
        # =====Timer
        self.tim_017hz = rospy.Timer(rospy.Duration(0.017), self.cllbck_tim_017hz)
        self.tim_1hz = rospy.Timer(rospy.Duration(1), self.cllbck_tim_1hz)
        self.tim_50hz = rospy.Timer(rospy.Duration(0.02), self.cllbck_tim_50hz)
        # =====Subscriber
        self.sub_db_data = rospy.Subscriber('db_data', db_data, self.cllbck_sub_db_data, queue_size=1)
        self.sub_fuels_input = rospy.Subscriber('fuels_input', fuels_input, self.cllbck_sub_fuels_input, queue_size=1)
        # =====Publisher
        self.pub_fuel_perminute = rospy.Publisher('fuel_perminute', String, queue_size=0)
        self.pub_fuel_perday = rospy.Publisher('fuel_perhour', String, queue_size=0)
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
        if not self.is_initialized:
            return

        # `self.fuel_sfc = rospy.get_param('fuel_sfc', 0.0)` is getting the value of the parameter
        # 'fuel_sfc' from the ROS parameter server using the `rospy.get_param()` function. If the
        # parameter is not found, it sets the default value to 0.0. The value of the parameter is then
        # assigned to the `self.fuel_sfc` variable.
        self.fuel_sfc = rospy.get_param('fuel_sfc', 0.0)

        num_of_suppliers = len(self.fuels_input.fuels_input)
        fuel_volume_needed = float(self.db_data.megawatt) * float(self.fuel_sfc) * 1000000.0

        if num_of_suppliers == 0:
            return

        # This code is creating a list of decision variables called `fuels` using the `LpVariable` function
        # from the `pulp` library. The number of decision variables created is equal to the number of fuel
        # suppliers (`num_of_suppliers`). Each decision variable is named after the corresponding fuel
        # supplier's name (`self.fuels_input.fuels_input[i].name`) and has a lower bound of the minimum volume
        # that the supplier can provide (`self.fuels_input.fuels_input[i].min_volume`) and an upper bound of
        # the maximum volume that the supplier can provide (`self.fuels_input.fuels_input[i].max_volume`).
        # These decision variables will be used in the optimization problem to determine the optimal amount of
        # fuel to purchase from each supplier.
        fuels = []
        for i in range(num_of_suppliers):
            fuels.append(LpVariable(self.fuels_input.fuels_input[i].name,
                                    self.fuels_input.fuels_input[i].min_volume,
                                    self.fuels_input.fuels_input[i].max_volume))

        # This code is defining an optimization problem using the `pulp` library.
        prob = LpProblem("PlantSense", LpMinimize)
        prob += (lpSum([fuels[i] * self.fuels_input.fuels_input[i].price for i in range(num_of_suppliers)]), "Total_Fuel_Cost")
        prob += (lpSum([fuels[i] for i in range(num_of_suppliers)]) == fuel_volume_needed, "Total_Fuel_Volume_Needed")

        # `status = prob.solve(PULP_CBC_CMD(msg=0))` is solving the optimization problem defined in
        # the `prob` variable using the PULP_CBC_CMD solver from the `pulp` library. The `msg=0`
        # argument is used to suppress the solver output. The `status` variable is assigned the status
        # of the optimization problem solution, which can be one of the following values: `Not
        # Solved`, `Optimal`, `Infeasible`, `Unbounded`, `Undefined`.
        status = prob.solve(PULP_CBC_CMD(msg=0))

        # This code is creating a dictionary called `result_json` that will be used to store the results
        # of the optimization problem. The `status` of the optimization problem is stored in the
        # `result_json` dictionary using the `LpStatus` function from the `pulp` library. The total fuel
        # cost is stored in the `result_json` dictionary using the `prob.objective.value()` function from
        # the `pulp` library. The volume of fuel purchased from each supplier is stored in the
        # `result_json` dictionary using the `prob.variables()` function from the `pulp` library.
        result_json = {}
        result_json['status'] = LpStatus[status]
        result_json['megawatt'] = self.db_data.megawatt
        result_json['fuel_sfc'] = self.fuel_sfc
        result_json['total_fuel_needed'] = fuel_volume_needed
        result_json['total_fuel_cost'] = prob.objective.value()
        result_json['fuels'] = []
        for v in prob.variables():
            result_json['fuels'].append({'name': v.name, 'volume': v.varValue})

        # This code is publishing the results of the optimization problem to the `fuel_perminute` topic
        # using the `String` message type. The `String` message type is used because the `fuel_perminute`
        # topic is subscribed to by the `fuel_perminute` node, which is written in JavaScript. The
        # `fuel_perminute` node is used to display the results of the optimization problem on the
        # PlantSense web application.
        msg_fuel_perminute = String()
        msg_fuel_perminute.data = str(result_json)
        self.pub_fuel_perminute.publish(msg_fuel_perminute)

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
