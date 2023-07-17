#!/usr/bin/python3

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
from pulp import LpMinimize, LpProblem, LpStatus, LpVariable, lpSum
from pulp.apis import PULP_CBC_CMD
from flask import Flask, request, jsonify
from flask_cors import CORS
from prometheus_client import start_http_server, Gauge


class Routine():
    def __init__(self):
        # =====Parameter
        self.fuel_sfc = rospy.get_param("fuel_sfc", 0.0)
        self.fuel_megawatt_value_manual = rospy.get_param("fuel_megawatt_value_manual", 0.0)
        self.fuel_megawatt_tag_manual = rospy.get_param("fuel_megawatt_tag_manual", "")
        # =====Timer
        self.tim_1hz = rospy.Timer(rospy.Duration(1), self.cllbck_tim_1hz)
        self.tim_2hz = rospy.Timer(rospy.Duration(0.5), self.cllbck_tim_2hz)
        # =====Subscriber
        self.sub_opcs = rospy.Subscriber("opcs", opcs, self.cllbck_sub_opcs, queue_size=1)
        # =====ServiceClient
        self.cli_db_insert = rospy.ServiceProxy("db_insert", db_insert)
        self.cli_db_select = rospy.ServiceProxy("db_select", db_select)
        self.cli_db_update = rospy.ServiceProxy("db_update", db_update)
        self.cli_db_upsert = rospy.ServiceProxy("db_upsert", db_upsert)
        self.cli_db_delete = rospy.ServiceProxy("db_delete", db_delete)

        self.isFirst1Min = True
        self.lastMinute = 0

        self.df_opcs_pool = pd.DataFrame(columns=["name", "value", "timestamp", "timestamp_local"])
        self.df_fuel_param = pd.DataFrame(columns=["name", "min_volume", "max_volume", "price"])
        self.gauge_opc_data = Gauge("opc_data", "opc_data", ["name"])

        self.megawatt_from_value_manual = 0.0
        self.megawatt_from_tag_manual = 0.0
        self.volume_from_value_manual = 0.0
        self.volume_from_tag_manual = 0.0

        if self.routine_init() == -1:
            rospy.signal_shutdown("")

    # --------------------------------------------------------------------------

    def cllbck_tim_1hz(self, event):
        for opc in self.df_opcs_pool.itertuples():
            self.cli_db_insert("tbl_data", ["name", "value", "timestamp"], [opc.name, str(opc.value), opc.timestamp])
            self.cli_db_insert("tbl_data_last60sec", ["name", "value", "timestamp"], [opc.name, str(opc.value), opc.timestamp])
            self.cli_db_insert("tbl_data_last1800sec", ["name", "value", "timestamp"], [opc.name, str(opc.value), opc.timestamp])
        self.cli_db_delete("tbl_data_last60sec", "timestamp_local < now() - interval '60 second'")
        self.cli_db_delete("tbl_data_last1800sec", "timestamp_local < now() - interval '1800 second'")

        # ==============================

        json_fuel_param = self.cli_db_select("tbl_fuel_param", ["name", "min_volume", "max_volume", "price"], "")
        self.df_fuel_param = pd.read_json(json_fuel_param.response, orient="split")

        # ----------

        self.fuel_sfc = rospy.get_param("fuel_sfc", 0.0)
        self.fuel_megawatt_value_manual = rospy.get_param("fuel_megawatt_value_manual", 0.0)
        self.fuel_megawatt_tag_manual = rospy.get_param("fuel_megawatt_tag_manual", "")

        # ----------

        tags = self.fuel_megawatt_tag_manual.split(";")
        indexes = self.df_opcs_pool[self.df_opcs_pool["name"].isin(tags)].index

        megawatt_value_from_tag = 0.0
        for index in indexes:
            megawatt_value_from_tag += self.df_opcs_pool.loc[index, "value"]

        # ----------

        self.megawatt_from_value_manual = float(self.fuel_megawatt_value_manual)
        self.megawatt_from_tag_manual = float(megawatt_value_from_tag)
        self.volume_from_value_manual = self.megawatt_from_value_manual * float(self.fuel_sfc) * 24.0
        self.volume_from_tag_manual = self.megawatt_from_tag_manual * float(self.fuel_sfc) * 24.0
        self.cli_db_upsert("tbl_param", ["name", "value"], ["fuel_megawatt_from_value_manual", str(self.megawatt_from_value_manual)], "name")
        self.cli_db_upsert("tbl_param", ["name", "value"], ["fuel_megawatt_from_tag_manual", str(self.megawatt_from_tag_manual)], "name")
        self.cli_db_upsert("tbl_param", ["name", "value"], ["fuel_volume_from_value_manual", str(self.volume_from_value_manual)], "name")
        self.cli_db_upsert("tbl_param", ["name", "value"], ["fuel_volume_from_tag_manual", str(self.volume_from_tag_manual)], "name")

    def cllbck_tim_2hz(self, event):
        if time.localtime().tm_sec != 0:
            return

        if self.lastMinute == time.localtime().tm_min:
            return

        self.lastMinute = time.localtime().tm_min

        # ----------

        self.result_from_value_manual = json.dumps(self.optimize_fuel(self.fuel_sfc, self.megawatt_from_value_manual, 1 / 1440), indent=2)
        self.result_from_tag_manual = json.dumps(self.optimize_fuel(self.fuel_sfc, self.megawatt_from_tag_manual, 1 / 1440), indent=2)
        self.cli_db_upsert("tbl_param", ["name", "value"], ["fuel_result_from_value_manual", str(self.result_from_value_manual)], "name")
        self.cli_db_upsert("tbl_param", ["name", "value"], ["fuel_result_from_tag_manual", str(self.result_from_tag_manual)], "name")

        # ----------

        self.cli_db_insert("tbl_fuel_realisasi", ["sfc", "mw", "result"], [str(self.fuel_sfc), str(self.megawatt_from_tag_manual), str(self.result_from_tag_manual)])

    # --------------------------------------------------------------------------

    def cllbck_sub_opcs(self, msg):
        for opc in msg.opcs:
            # Find in self.opcs_pool to check whether the opc is already in the pool
            indexes = self.df_opcs_pool[self.df_opcs_pool["name"] == opc.name].index

            # If the opc is already in the pool, update the value and timestamp in the pool
            if len(indexes) != 0:
                if self.df_opcs_pool.loc[indexes[0], "timestamp"] < opc.timestamp:
                    self.df_opcs_pool.loc[indexes[0]] = [opc.name, opc.value, opc.timestamp, time.time()]
                    self.cli_db_upsert("tbl_data_last", ["name", "value", "timestamp"], [opc.name, str(opc.value), opc.timestamp], "name")
                    self.gauge_opc_data.labels(opc.name).set(opc.value)
            # If the opc is not in the pool, insert the opc into the pool
            else:
                self.df_opcs_pool.loc[len(self.df_opcs_pool)] = [opc.name, opc.value, opc.timestamp, time.time()]
                self.cli_db_upsert("tbl_data_last", ["name", "value", "timestamp"], [opc.name, str(opc.value), opc.timestamp], "name")
                self.gauge_opc_data.labels(opc.name).set(opc.value)

    # --------------------------------------------------------------------------

    def routine_init(self):
        time.sleep(2.0)

        # Prometheus
        start_http_server(5001)

        # Flask
        flask_thread = threading.Thread(target=self.thread_flask)
        flask_thread.daemon = True
        flask_thread.start()

        return 0

    # --------------------------------------------------------------------------

    def optimize_fuel(self, sfc: float, megawatt: float, period: float):
        number_of_variables = len(self.df_fuel_param)

        variables = [LpVariable(self.df_fuel_param["name"][i],
                                self.df_fuel_param["min_volume"][i] * period / 24,
                                self.df_fuel_param["max_volume"][i] * period / 24) for i in range(number_of_variables)]

        problem = LpProblem("Optimization", LpMinimize)
        problem += lpSum([self.df_fuel_param["price"][i] * variables[i] for i in range(number_of_variables)])
        problem += lpSum([variables[i] for i in range(number_of_variables)]) == float(sfc) * float(megawatt) * float(period)

        status = problem.solve(PULP_CBC_CMD(msg=0))

        result = {}
        result["status"] = LpStatus[status]
        result["sfc"] = float(sfc)
        result["megawatt"] = float(megawatt)
        result["volume"] = float(sfc) * float(megawatt) * 24.0
        result["period"] = float(period)
        result["cost"] = problem.objective.value()
        result["purchase"] = {}
        for i in range(number_of_variables):
            result["purchase"][self.df_fuel_param["name"][i]] = variables[i].value()

        return result

    def trigger_fuel(self):
        date = time.strftime("%Y-%m-%d", time.localtime())

        json_row = self.cli_db_select("tbl_fuel_rencana", ["*"], "date = '" + str(date) + "'")
        df_row = pd.read_json(json_row.response, orient="split")

        # ----------

        if len(df_row) != 1:
            return "ERROR"

        # ----------

        df_row["sfc"][0] = self.fuel_sfc
        df_row["value_total"][0] = json.dumps(self.optimize_fuel(df_row["sfc"][0], df_row["mw_total"][0], 24))
        for i in range(48):
            df_row["value" + str(i)][0] = json.dumps(self.optimize_fuel(df_row["sfc"][0], df_row["mw" + str(i)][0], 0.5))

        columns = []
        for i in df_row.columns.tolist()[2:]:
            columns.append(str(i))
        values = []
        for i in df_row.values.tolist()[0][2:]:
            values.append(str(i))

        self.cli_db_delete("tbl_fuel_rencana_last", "")
        self.cli_db_insert("tbl_fuel_rencana_last", columns, values)
        self.cli_db_update("tbl_fuel_rencana", columns, values, "date = '" + str(date) + "'")

        return "OK"

    # --------------------------------------------------------------------------

    app = Flask(__name__)
    CORS(app)

    @app.route("/trigger", methods=["GET"])
    def flask_index():
        result = routine.trigger_fuel()
        return str(result)

    @app.route("/optimize", methods=["GET"])
    def flask_optimize():
        param = {}
        param["sfc"] = float(request.form["sfc"])
        param["megawatt"] = float(request.form["megawatt"])
        param["period"] = float(request.form["period"])

        result = routine.optimize_fuel(param["sfc"], param["megawatt"], param["period"])
        return jsonify(result)

    def thread_flask(self):
        self.app.run(host="0.0.0.0", port=5000)


if __name__ == "__main__":
    rospy.init_node("routine", anonymous=False)
    routine = Routine()
    rospy.spin()
