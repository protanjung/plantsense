#!/usr/bin/python

import rospy
from ps_interface.msg import opc, opcs
from ps_interface.srv import db_insert, db_insertResponse
from ps_interface.srv import db_select, db_selectResponse
from ps_interface.srv import db_update, db_updateResponse
from ps_interface.srv import db_upsert, db_upsertResponse
from ps_interface.srv import db_delete, db_deleteResponse
import time
import json
import threading
import pandas as pd
from flask import Flask, request, jsonify
from pulp import LpMinimize, LpProblem, LpStatus, LpVariable, lpSum
from pulp.apis import PULP_CBC_CMD


class Routine():
    def __init__(self):
        # =====Parameter
        self.fuel_sfc = rospy.get_param("fuel_sfc", 0.0)
        self.fuel_megawatt_value_manual = rospy.get_param("fuel_megawatt_value_manual", 0.0)
        self.fuel_megawatt_tag_manual = rospy.get_param("fuel_megawatt_tag_manual", "")
        # =====Timer
        self.tim_1hz = rospy.Timer(rospy.Duration(1), self.cllbck_tim_1hz)
        # =====Subscriber
        self.sub_opcs = rospy.Subscriber("opcs", opcs, self.cllbck_sub_opcs, queue_size=1)
        # =====ServiceClient
        self.cli_db_insert = rospy.ServiceProxy("db_insert", db_insert)
        self.cli_db_select = rospy.ServiceProxy("db_select", db_select)
        self.cli_db_update = rospy.ServiceProxy("db_update", db_update)
        self.cli_db_upsert = rospy.ServiceProxy("db_upsert", db_upsert)
        self.cli_db_delete = rospy.ServiceProxy("db_delete", db_delete)

        self.opcs_pool = pd.DataFrame(columns=["name", "value", "timestamp", "timestamp_local"])
        self.fuel_param = pd.DataFrame(columns=["name", "min_volume", "max_volume", "price"])

        if self.routine_init() == -1:
            rospy.signal_shutdown("")

    # --------------------------------------------------------------------------

    def cllbck_tim_1hz(self, event):
        for opc in self.opcs_pool.itertuples():
            self.cli_db_insert("tbl_data",
                               ["name", "value", "timestamp"],
                               [opc.name, str(opc.value), opc.timestamp])
            self.cli_db_insert("tbl_data_last60sec",
                               ["name", "value", "timestamp"],
                               [opc.name, str(opc.value), opc.timestamp])
            self.cli_db_insert("tbl_data_last1800sec",
                               ["name", "value", "timestamp"],
                               [opc.name, str(opc.value), opc.timestamp])
        self.cli_db_delete("tbl_data_last60sec", "timestamp_local < now() - interval '60 second'")
        self.cli_db_delete("tbl_data_last1800sec", "timestamp_local < now() - interval '1800 second'")

        # ==============================

        response_json = self.cli_db_select("tbl_fuel_param", ["name", "min_volume", "max_volume", "price"], "").response
        self.fuel_param = pd.read_json(response_json, orient="split")

        # ==============================

        self.fuel_sfc = rospy.get_param("fuel_sfc", 0.0)
        self.fuel_megawatt_value_manual = rospy.get_param("fuel_megawatt_value_manual", 0.0)
        self.fuel_megawatt_tag_manual = rospy.get_param("fuel_megawatt_tag_manual", "")

        # ==============================

        where = ""
        for tag in self.fuel_megawatt_tag_manual.split(";"):
            where += "name = '" + tag + "' OR "
        where = where[:-4]

        response_json = self.cli_db_select("tbl_data_last", ["name", "value", "timestamp"], where).response
        response_df = pd.read_json(response_json, orient="split")

        self.megawatt_from_value_manual = float(self.fuel_megawatt_value_manual)
        self.megawatt_from_tag_manual = float(response_df.sum(numeric_only=True)["value"])
        self.volume_from_value_manual = self.megawatt_from_value_manual * float(self.fuel_sfc) * 1000  # TODO: Check formula, ask them!
        self.volume_from_tag_manual = self.megawatt_from_tag_manual * float(self.fuel_sfc) * 1000  # TODO: Check formula, ask them!
        self.cli_db_upsert("tbl_param", ["name", "value"], ["fuel_volume_from_value_manual", str(self.volume_from_value_manual)], "name")
        self.cli_db_upsert("tbl_param", ["name", "value"], ["fuel_volume_from_tag_manual", str(self.volume_from_tag_manual)], "name")

        # ==============================

        if time.localtime().tm_sec != 0:
            return

        # ==============================

        result_total_dict = {}
        result_total_dict["by_value"] = self.optimize_fuel(self.megawatt_from_value_manual, self.volume_from_value_manual)
        result_total_dict["by_tag"] = self.optimize_fuel(self.megawatt_from_tag_manual, self.volume_from_tag_manual)
        result_total_json = json.dumps(result_total_dict, indent=2)

        self.cli_db_delete("tbl_fuel_last", "")
        self.cli_db_insert("tbl_fuel_last", ["result"], [result_total_json])
        self.cli_db_insert("tbl_fuel", ["result"], [result_total_json])

    # --------------------------------------------------------------------------

    def cllbck_sub_opcs(self, msg):
        for opc in msg.opcs:
            # Find in self.opcs_pool to check whether the opc is already in the pool
            index = self.opcs_pool[self.opcs_pool["name"] == opc.name].index

            if len(index) != 0:
                # If the opc is already in the pool, check whether the timestamp is changed
                # If the value is changed, update the value and timestamp on the database
                if self.opcs_pool.loc[index[0], "timestamp"] < opc.timestamp:
                    self.cli_db_upsert("tbl_data_last",
                                       ["name", "value", "timestamp"],
                                       [opc.name, str(opc.value), opc.timestamp],
                                       "name")
            else:
                # If the opc is not in the pool, update the value and timestamp on the database
                self.cli_db_upsert("tbl_data_last",
                                   ["name", "value", "timestamp"],
                                   [opc.name, str(opc.value), opc.timestamp],
                                   "name")

            if len(index) != 0:
                # If the opc is already in the pool, update the value and timestamp in the pool
                self.opcs_pool.loc[index[0]] = [opc.name, opc.value, opc.timestamp, time.time()]
            else:
                # If the opc is not in the pool, insert the opc into the pool
                self.opcs_pool.loc[len(self.opcs_pool)] = [opc.name, opc.value, opc.timestamp, time.time()]

    # --------------------------------------------------------------------------

    def routine_init(self):
        time.sleep(2)

        flask_thread = threading.Thread(target=self.thread_flask)
        flask_thread.daemon = True
        flask_thread.start()

        return 0

    def optimize_fuel(self, megawatt, volume):
        num_of_variable = len(self.fuel_param)

        variable = [LpVariable(self.fuel_param["name"][i],
                               self.fuel_param["min_volume"][i],
                               self.fuel_param["max_volume"][i]) for i in range(num_of_variable)]

        problem = LpProblem("Optimization", LpMinimize)
        problem += lpSum([self.fuel_param["price"][i] * variable[i] for i in range(num_of_variable)])
        problem += lpSum([variable[i] for i in range(num_of_variable)]) == volume

        status = problem.solve(PULP_CBC_CMD(msg=0))

        result = {}
        result["status"] = LpStatus[status]
        result["megawatt"] = megawatt
        result["fuel_volume"] = volume
        result["fuel_cost"] = problem.objective.value()
        result["fuel_purchase"] = {}
        for i in range(num_of_variable):
            result["fuel_purchase"][self.fuel_param["name"][i]] = variable[i].value()

        return result

    # --------------------------------------------------------------------------

    app = Flask(__name__)

    @app.route("/optimize", methods=["POST"])
    def flask_optimize():
        param = {}
        param["megawatt"] = float(request.form["megawatt"])
        param["sfc"] = float(request.form["sfc"])

        volume = param["megawatt"] * param["sfc"] * 1000  # TODO: Check formula, ask them!
        result = routine.optimize_fuel(param["megawatt"], volume)

        return jsonify(result)

    def thread_flask(self):
        self.app.run(host="0.0.0.0", port=5000)


if __name__ == "__main__":
    rospy.init_node("routine", anonymous=False)
    routine = Routine()
    rospy.spin()
