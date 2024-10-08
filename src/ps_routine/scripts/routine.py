#!/usr/bin/python3

import rospy
from ps_interface.msg import opc, opcs
from ps_interface.srv import db_insert, db_insertResponse
from ps_interface.srv import db_select, db_selectResponse
from ps_interface.srv import db_update, db_updateResponse
from ps_interface.srv import db_upsert, db_upsertResponse
from ps_interface.srv import db_delete, db_deleteResponse
import os
import sys
import time
import json
import threading
import tabula
import pandas as pd
from flask import Flask, request, jsonify
from flask_cors import CORS
from pulp import LpMinimize, LpProblem, LpStatus, LpVariable, lpSum
from pulp.apis import PULP_CBC_CMD
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

        self.isFirst1Minute = True
        self.last1Minute = 0

        self.df_opcs_pool = pd.DataFrame(columns=["name", "value", "timestamp", "timestamp_local"])
        self.df_fuel_param_active = pd.DataFrame(columns=["name", "min_volume", "max_volume", "price"])
        self.df_fuel_param_queue = pd.DataFrame(columns=["name", "min_volume", "max_volume", "price"])
        self.gauge_opc_data = Gauge("opc_data", "opc_data", ["name"])

        self.megawatt_from_value_manual = 0.0
        self.megawatt_from_tag_manual = 0.0
        self.volume_from_value_manual = 0.0
        self.volume_from_tag_manual = 0.0

        if self.routine_init() == -1:
            rospy.signal_shutdown("")

    # --------------------------------------------------------------------------

    def cllbck_tim_1hz(self, event):
        try:
            for opc in self.df_opcs_pool.itertuples():
                self.cli_db_insert("tbl_data", ["name", "value", "timestamp"], [opc.name, str(opc.value), opc.timestamp])
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            rospy.logerr("Error: " + str(e) + " at " + str(exc_tb.tb_lineno))

        # ==============================

        try:
            json_fuel_param = self.cli_db_select("tbl_fuel_param_active", ["name", "min_volume", "max_volume", "price"], "")
            self.df_fuel_param_active = pd.read_json(json_fuel_param.response, orient="split")
            json_fuel_param = self.cli_db_select("tbl_fuel_param_queue", ["name", "min_volume", "max_volume", "price"], "")
            self.df_fuel_param_queue = pd.read_json(json_fuel_param.response, orient="split")
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            rospy.logerr("Error: " + str(e) + " at " + str(exc_tb.tb_lineno))

        # ----------

        try:
            self.fuel_sfc = rospy.get_param("fuel_sfc", 0.0)
            self.fuel_megawatt_value_manual = rospy.get_param("fuel_megawatt_value_manual", 0.0)
            self.fuel_megawatt_tag_manual = rospy.get_param("fuel_megawatt_tag_manual", "")
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            rospy.logerr("Error: " + str(e) + " at " + str(exc_tb.tb_lineno))

        # ----------

        try:
            tags = self.fuel_megawatt_tag_manual.split(";")
            indexes = self.df_opcs_pool[self.df_opcs_pool["name"].isin(tags)].index

            megawatt_value_from_tag = 0.0
            for index in indexes:
                megawatt_value_from_tag += self.df_opcs_pool.loc[index, "value"]
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            rospy.logerr("Error: " + str(e) + " at " + str(exc_tb.tb_lineno))

        # ----------

        try:
            self.megawatt_from_value_manual = float(self.fuel_megawatt_value_manual)
            self.megawatt_from_tag_manual = float(megawatt_value_from_tag)
            self.volume_from_value_manual = self.megawatt_from_value_manual * float(self.fuel_sfc) * 24.0
            self.volume_from_tag_manual = self.megawatt_from_tag_manual * float(self.fuel_sfc) * 24.0
            self.cli_db_upsert("tbl_param", ["name", "value"], ["fuel_megawatt_from_value_manual", str(self.megawatt_from_value_manual)], "name")
            self.cli_db_upsert("tbl_param", ["name", "value"], ["fuel_megawatt_from_tag_manual", str(self.megawatt_from_tag_manual)], "name")
            self.cli_db_upsert("tbl_param", ["name", "value"], ["fuel_volume_from_value_manual", str(self.volume_from_value_manual)], "name")
            self.cli_db_upsert("tbl_param", ["name", "value"], ["fuel_volume_from_tag_manual", str(self.volume_from_tag_manual)], "name")
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            rospy.logerr("Error: " + str(e) + " at " + str(exc_tb.tb_lineno))

    def cllbck_tim_2hz(self, event):
        if time.localtime().tm_sec != 0:
            return

        if self.last1Minute == time.localtime().tm_min:
            return

        self.last1Minute = time.localtime().tm_min

        # ----------

        try:
            # If midnight, delete all data in tbl_fuel_param_active and insert all data in tbl_fuel_param_queue
            if time.localtime().tm_hour == 0 and time.localtime().tm_min == 0:
                # Delete all data in tbl_fuel_param_active
                self.cli_db_delete("tbl_fuel_param_active", "")
                # Insert all data in tbl_fuel_param_queue
                for i in range(self.df_fuel_param_queue.shape[0]):
                    self.cli_db_insert("tbl_fuel_param_active", ["name", "min_volume", "max_volume", "price"], [str(x) for x in self.df_fuel_param_queue.iloc[i].tolist()])
                # Copy all data in tbl_fuel_param_queue to tbl_fuel_param_active
                self.df_fuel_param_active = self.df_fuel_param_queue.copy()
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            rospy.logerr("Error: " + str(e) + " at " + str(exc_tb.tb_lineno))

        # ----------

        try:
            self.result_from_value_manual = json.dumps(self.optimize_fuel_queue(self.fuel_sfc, self.megawatt_from_value_manual), indent=2)
            self.result_from_tag_manual = json.dumps(self.optimize_fuel_queue(self.fuel_sfc, self.megawatt_from_tag_manual), indent=2)
            self.cli_db_upsert("tbl_param", ["name", "value"], ["fuel_result_from_value_manual", str(self.result_from_value_manual)], "name")
            self.cli_db_upsert("tbl_param", ["name", "value"], ["fuel_result_from_tag_manual", str(self.result_from_tag_manual)], "name")
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            rospy.logerr("Error: " + str(e) + " at " + str(exc_tb.tb_lineno))

        # ----------

        try:
            self.cli_db_insert("tbl_fuel_realisasi", ["sfc", "mw", "result"], [str(self.fuel_sfc), str(self.megawatt_from_tag_manual), str(self.result_from_tag_manual)])
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            rospy.logerr("Error: " + str(e) + " at " + str(exc_tb.tb_lineno))

    # --------------------------------------------------------------------------

    def cllbck_sub_opcs(self, msg):
        for opc in msg.opcs:
            # Find in self.opcs_pool to check whether the opc is already in the pool
            indexes = self.df_opcs_pool[self.df_opcs_pool["name"] == opc.name].index

            # If the opc is already in the pool, update the value and timestamp in the pool
            if len(indexes) != 0:
                if self.df_opcs_pool.loc[indexes[0], "timestamp"] < opc.timestamp:
                    self.df_opcs_pool.loc[indexes[0]] = [opc.name, opc.value, opc.timestamp, time.time()]
                    self.gauge_opc_data.labels(opc.name).set(opc.value)
            # If the opc is not in the pool, insert the opc into the pool
            else:
                self.df_opcs_pool.loc[len(self.df_opcs_pool)] = [opc.name, opc.value, opc.timestamp, time.time()]
                self.gauge_opc_data.labels(opc.name).set(opc.value)

    # --------------------------------------------------------------------------

    def routine_init(self):
        time.sleep(10.0)

        # Flask
        flask_thread = threading.Thread(target=self.thread_flask)
        flask_thread.daemon = True
        flask_thread.start()

        # Prometheus
        start_http_server(8800)

        return 0

    # --------------------------------------------------------------------------

    def pdf_correct_inconsistent_data(self, df):
        buffer = df.copy()

        # Detect error by checking if there is a space in column header
        i_error = -1
        for i, column in enumerate(buffer.columns):
            if " " in column:
                i_error = i
                break

        # Correct error by splitting column header and value
        if i_error != -1:
            column0_header = buffer.columns[i_error].split(" ")[0]
            column0_data = buffer.iloc[:, i_error].str.split(" ").str[0]
            column1_header = buffer.columns[i_error].split(" ")[1]
            column1_data = buffer.iloc[:, i_error].str.split(" ").str[1]
            buffer.drop(columns=[buffer.columns[i_error]], inplace=True)
            buffer.insert(i_error + 0, column0_header, column0_data)
            buffer.insert(i_error + 1, column1_header, column1_data)

        # Convert all data to numeric
        try:
            buffer = buffer.apply(pd.to_numeric, errors="coerce")
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            rospy.logerr("Error: " + str(e) + " at " + str(exc_tb.tb_lineno))
            return -1, buffer

        return 0, buffer

    def pdf_remove_unwanted_data(self, df):
        buffer = df.copy()

        # Remove rows with column 0 and 1 values containing NaN
        buffer.dropna(subset=[buffer.columns[0], buffer.columns[1]], inplace=True)
        # Remove rows with column 0 values not containing 'PLTG', 'PLTGU', or 'PLTU'
        row_indices = buffer[~buffer.iloc[:, 0].str.contains("PLTG|PLTGU|PLTU")].index.tolist()
        buffer.drop(row_indices, inplace=True)
        # Remove rows with column 1 values not containing 'GRESIK', or 'GRSIK'
        row_indices = buffer[~buffer.iloc[:, 1].str.contains("GRESIK|GRSIK")].index.tolist()
        buffer.drop(row_indices, inplace=True)
        # Remove unnamed columns
        column_indices = buffer.columns[buffer.columns.str.contains("Unnamed")].tolist()
        buffer.drop(columns=column_indices, inplace=True)
        # Remove column 'Jam' or 'Rata-2' if exists
        if "Jam" in buffer.columns:
            buffer.drop(columns=["Jam"], inplace=True)
        if "Rata-2" in buffer.columns:
            buffer.drop(columns=["Rata-2"], inplace=True)
        # Reset index
        buffer.reset_index(drop=True, inplace=True)

        return 0, buffer

    def pdf_parse_data(self):
        try:
            filename = os.path.join(os.path.expanduser("~"), "plantsense_roh.pdf")
            df_siang = tabula.read_pdf(filename, pages=10)[0]
            df_malam = tabula.read_pdf(filename, pages=19)[0]
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            rospy.logerr("Error: " + str(e) + " at " + str(exc_tb.tb_lineno))
            return {"status": -1, "message": "Error: " + str(e) + " at " + str(exc_tb.tb_lineno)}, []

        # Check if table column count is correct
        if len(df_siang.columns) != 28 or len(df_malam.columns) != 27:
            return {"status": -1, "message": "Jumlah kolom tidak sesuai"}, []

        # Remove unwanted data
        _, df_siang = self.pdf_remove_unwanted_data(df_siang)
        _, df_malam = self.pdf_remove_unwanted_data(df_malam)

        # Check if table row count is correct
        if len(df_siang) != len(df_malam):
            return {"status": -1, "message": "Jumlah baris tidak sesuai"}, []

        # Correct inconsistent data
        result, df_siang = self.pdf_correct_inconsistent_data(df_siang)
        if result == -1:
            return {"status": -1, "message": "Data tidak konsisten"}, []
        result, df_malam = self.pdf_correct_inconsistent_data(df_malam)
        if result == -1:
            return {"status": -1, "message": "Data tidak konsisten"}, []

        # Check if table column count is correct
        if len(df_siang.columns) + len(df_malam.columns) != 48:
            return {"status": -1, "message": "Jumlah kolom tidak sesuai"}, []

        # Combine siang and malam data
        df = pd.concat([df_siang, df_malam], axis=1)

        return {"status": 0, "message": "Berhasil"}, df.sum(axis=0).tolist()

    # --------------------------------------------------------------------------

    def optimize_fuel_active(self, sfc, megawatt):
        number_of_variables = len(self.df_fuel_param_active)

        variables = [LpVariable(self.df_fuel_param_active["name"][i],
                                self.df_fuel_param_active["min_volume"][i],
                                self.df_fuel_param_active["max_volume"][i]) for i in range(number_of_variables)]

        problem = LpProblem("Optimization", LpMinimize)
        problem += lpSum([self.df_fuel_param_active["price"][i] * variables[i] for i in range(number_of_variables)])
        problem += lpSum([variables[i] for i in range(number_of_variables)]) == float(sfc) * float(megawatt) * 24.0 * 1000.0

        status = problem.solve(PULP_CBC_CMD(msg=0))

        result = {}
        result["status"] = LpStatus[status]
        result["sfc"] = float(sfc)
        result["megawatt"] = float(megawatt)
        result["volume"] = float(sfc) * float(megawatt) * 24.0
        result["cost"] = problem.objective.value()
        result["purchase"] = {}
        for i in range(number_of_variables):
            result["purchase"][self.df_fuel_param_active["name"][i]] = variables[i].value()

        return result

    def optimize_fuel_queue(self, sfc, megawatt):
        number_of_variables = len(self.df_fuel_param_queue)

        variables = [LpVariable(self.df_fuel_param_queue["name"][i],
                                self.df_fuel_param_queue["min_volume"][i],
                                self.df_fuel_param_queue["max_volume"][i]) for i in range(number_of_variables)]

        problem = LpProblem("Optimization", LpMinimize)
        problem += lpSum([self.df_fuel_param_queue["price"][i] * variables[i] for i in range(number_of_variables)])
        problem += lpSum([variables[i] for i in range(number_of_variables)]) == float(sfc) * float(megawatt) * 24.0 * 1000.0

        status = problem.solve(PULP_CBC_CMD(msg=0))

        result = {}
        result["status"] = LpStatus[status]
        result["sfc"] = float(sfc)
        result["megawatt"] = float(megawatt)
        result["volume"] = float(sfc) * float(megawatt) * 24.0
        result["cost"] = problem.objective.value()
        result["purchase"] = {}
        for i in range(number_of_variables):
            result["purchase"][self.df_fuel_param_queue["name"][i]] = variables[i].value()

        return result

    def trigger_fuel(self, date=None, sfc=None):
        if date is None:
            date = time.strftime("%Y-%m-%d", time.localtime())
        if sfc is None:
            sfc = self.fuel_sfc

        # ----------

        json_row = self.cli_db_select("tbl_fuel_rencana", ["*"], "date = '" + str(date) + "'")
        df_row = pd.read_json(json_row.response, orient="split")

        # ----------

        if df_row.shape[0] == 0:
            return {"status": -1, "message": "Data tidak ditemukan"}
        if df_row.shape[0] > 1:
            return {"status": -1, "message": "Data yang ditemukan lebih dari satu"}

        # ----------

        try:
            df_row.loc[0, "date"] = str(date)
            df_row.loc[0, "sfc"] = str(sfc)
            for i in range(48):
                df_row.loc[0, "value" + str(i)] = json.dumps(self.optimize_fuel_queue(sfc, df_row.loc[0, "mw" + str(i)]), indent=2)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            return {"status": -1, "message": "Error: " + str(e) + " at " + str(exc_tb.tb_lineno)}

        # ----------

        columns = []
        for i in df_row.columns.tolist()[2:]:
            columns.append(str(i))
        values = []
        for i in df_row.values.tolist()[0][2:]:
            values.append(str(i))

        self.cli_db_update("tbl_fuel_rencana", columns, values, "date = '" + str(date) + "'")

        # ----------

        for i in range(48):
            _timestamp = str(date) + " " + str(i // 2).zfill(2) + ":" + str(i % 2 * 30).zfill(2) + ":00"
            _sfc = str(df_row.loc[0, "sfc"])
            _mw = str(df_row.loc[0, "mw" + str(i)])
            _result = str(df_row.loc[0, "value" + str(i)])

            self.cli_db_upsert("tbl_fuel_rencana_simple", ["timestamp", "sfc", "mw", "result"], [_timestamp, _sfc, _mw, _result], "timestamp")

        # ----------

        return {"status": 0, "message": "Berhasil"}

    def trigger_fuel_pdf(self, date=None, sfc=None):
        if date is None:
            date = time.strftime("%Y-%m-%d", time.localtime())
        if sfc is None:
            sfc = self.fuel_sfc

        # ----------

        result, megawatt = self.pdf_parse_data()
        if len(megawatt) == 0:
            return result

        # ----------

        header = []
        header.append("date")
        header.append("sfc")
        for i in range(48):
            header.append("mw" + str(i))
            header.append("value" + str(i))
        header.append("mw_total")
        header.append("value_total")

        df_row = pd.DataFrame(columns=header)

        try:
            df_row.loc[0, "date"] = str(date)
            df_row.loc[0, "sfc"] = str(sfc)
            for i in range(48):
                df_row.loc[0, "mw" + str(i)] = str(megawatt[i])
                df_row.loc[0, "value" + str(i)] = json.dumps(self.optimize_fuel_queue(sfc, megawatt[i]), indent=2)
            df_row.loc[0, "mw_total"] = str(sum(megawatt))
            df_row.loc[0, "value_total"] = "{}"
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            return {"status": -1, "message": "Error: " + str(e) + " at " + str(exc_tb.tb_lineno)}

        # ----------

        columns = []
        for i in df_row.columns.tolist():
            columns.append(str(i))
        values = []
        for i in df_row.values.tolist()[0]:
            values.append(str(i))

        self.cli_db_upsert("tbl_fuel_rencana", columns, values, "date")

        # ----------

        for i in range(48):
            _timestamp = str(date) + " " + str(i // 2).zfill(2) + ":" + str(i % 2 * 30).zfill(2) + ":00"
            _sfc = str(df_row.loc[0, "sfc"])
            _mw = str(df_row.loc[0, "mw" + str(i)])
            _result = str(df_row.loc[0, "value" + str(i)])

            self.cli_db_upsert("tbl_fuel_rencana_simple", ["timestamp", "sfc", "mw", "result"], [_timestamp, _sfc, _mw, _result], "timestamp")

        # ----------

        return {"status": 0, "message": "Berhasil"}

    # --------------------------------------------------------------------------

    app = Flask(__name__)
    CORS(app)

    @app.route("/trigger", methods=["POST"])
    def flask_trigger():
        param = {}
        param["date"] = str(request.form["date"]) if "date" in request.form else None
        param["sfc"] = str(request.form["sfc"]) if "sfc" in request.form else None

        result = routine.trigger_fuel(param["date"], param["sfc"])
        return jsonify(result)

    @app.route("/trigger_pdf", methods=["POST"])
    def flask_trigger_pdf():
        param = {}
        param["date"] = str(request.form["date"]) if "date" in request.form else None
        param["sfc"] = str(request.form["sfc"]) if "sfc" in request.form else None

        # Check if file parameter is empty
        if 'file' not in request.files:
            return jsonify({"status": -1, "message": "Parameter 'file' harus diisi"})

        file = request.files['file']

        # Check if file is empty
        if file.filename == '':
            return jsonify({"status": -1, "message": "File tidak boleh kosong"})
        # Check if file is PDF
        if file.filename.split(".")[-1].lower() != "pdf":
            return jsonify({"status": -1, "message": "File harus berformat PDF"})

        filename = os.path.join(os.path.expanduser("~"), "plantsense_roh.pdf")
        file.save(filename)

        result = routine.trigger_fuel_pdf(param["date"], param["sfc"])
        return jsonify(result)

    @app.route("/optimize", methods=["POST"])
    def flask_optimize():
        param = {}
        param["sfc"] = float(request.form["sfc"])
        param["megawatt"] = float(request.form["megawatt"])

        result = routine.optimize_fuel_active(param["sfc"], param["megawatt"])
        return jsonify(result)

    @app.route("/optimize_active", methods=["POST"])
    def flask_optimize_active():
        param = {}
        param["sfc"] = float(request.form["sfc"])
        param["megawatt"] = float(request.form["megawatt"])

        result = routine.optimize_fuel_active(param["sfc"], param["megawatt"])
        return jsonify(result)

    @app.route("/optimize_queue", methods=["POST"])
    def flask_optimize_queue():
        param = {}
        param["sfc"] = float(request.form["sfc"])
        param["megawatt"] = float(request.form["megawatt"])

        result = routine.optimize_fuel_queue(param["sfc"], param["megawatt"])
        return jsonify(result)

    def thread_flask(self):
        self.app.run(host="0.0.0.0", port=5000)


if __name__ == "__main__":
    rospy.init_node("routine", anonymous=False)
    routine = Routine()
    rospy.spin()
