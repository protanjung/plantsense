#!/usr/bin/python3

import rospy
from ps_interface.srv import db_insert, db_insertResponse
from ps_interface.srv import db_select, db_selectResponse
from ps_interface.srv import db_update, db_updateResponse
from ps_interface.srv import db_upsert, db_upsertResponse
from ps_interface.srv import db_delete, db_deleteResponse
import sys
from threading import Lock
import psycopg2
import pandas as pd


class InterfaceDatabase():
    def __init__(self, is_create_table=False):
        # =====Parameter
        self.db_host = rospy.get_param("db/host", "postgres")
        self.db_port = rospy.get_param("db/port", 5432)
        self.db_user = rospy.get_param("db/user", "opc")
        self.db_password = rospy.get_param("db/password", "opc")
        self.db_database = rospy.get_param("db/database", "opc")
        self.db_schema = rospy.get_param("db/schema", "public")
        # =====Timer
        self.tim_1hz = rospy.Timer(rospy.Duration(1), self.cllbck_tim_1hz)
        # =====ServiceServer
        self.srv_db_insert = rospy.Service("db_insert", db_insert, self.cllbck_srv_db_insert)
        self.srv_db_select = rospy.Service("db_select", db_select, self.cllbck_srv_db_select)
        self.srv_db_update = rospy.Service("db_update", db_update, self.cllbck_srv_db_update)
        self.srv_db_upsert = rospy.Service("db_upsert", db_upsert, self.cllbck_srv_db_upsert)
        self.srv_db_delete = rospy.Service("db_delete", db_delete, self.cllbck_srv_db_delete)
        # =====Mutex
        self.mutex_db = Lock()

        self.is_create_table = is_create_table

        if self.interface_database_init() == -1:
            rospy.signal_shutdown("")

    # --------------------------------------------------------------------------

    def cllbck_tim_1hz(self, event):
        response_json = self.db_select(self.db_schema, "tbl_param", ["name", "value"], "")
        response_df = pd.read_json(response_json, orient="split")

        for i in range(len(response_df)):
            rospy.set_param(str(response_df["name"][i]), str(response_df["value"][i]))

    # --------------------------------------------------------------------------

    def cllbck_srv_db_insert(self, req):
        res = db_insertResponse()
        try:
            self.db_insert(self.db_schema, req.table_name, req.columns, req.values)
        except BaseException as e:
            rospy.logerr("db_insert: " + str(e))
        return res

    def cllbck_srv_db_select(self, req):
        res = db_selectResponse()
        try:
            res.response = self.db_select(self.db_schema, req.table_name, req.columns, req.where)
        except BaseException as e:
            rospy.logerr("db_select: " + str(e))
        return res

    def cllbck_srv_db_update(self, req):
        res = db_updateResponse()
        try:
            self.db_update(self.db_schema, req.table_name, req.columns, req.values, req.where)
        except BaseException as e:
            rospy.logerr("db_update: " + str(e))
        return res

    def cllbck_srv_db_upsert(self, req):
        res = db_upsertResponse()
        try:
            self.db_upsert(self.db_schema, req.table_name, req.columns, req.values, req.primary_key)
        except BaseException as e:
            rospy.logerr("db_upsert: " + str(e))
        return res

    def cllbck_srv_db_delete(self, req):
        res = db_deleteResponse()
        try:
            self.db_delete(self.db_schema, req.table_name, req.where)
        except BaseException as e:
            rospy.logerr("db_delete: " + str(e))
        return res

    # --------------------------------------------------------------------------

    def interface_database_init(self):
        self.mutex_db.acquire()
        self.myDatabase = psycopg2.connect(
            host=self.db_host,
            port=self.db_port,
            user=self.db_user,
            password=self.db_password,
            database=self.db_database
        )
        self.myCursor = self.myDatabase.cursor()
        self.mutex_db.release()

        # ==============================
        # Param
        # ==============================

        tbl_param_names = ["name", "value"]
        tbl_param_parameters = ["VARCHAR NOT NULL PRIMARY KEY", "VARCHAR NOT NULL"]
        self.db_create_table(self.db_schema, "tbl_param", tbl_param_names, tbl_param_parameters)

        self.db_insert_if_not_exists(self.db_schema, "tbl_param", ["name", "value"], ["fuel_megawatt_tag_manual", "P.B11GTHRSG.G1LA00264;P.B12GTHRSG.G2LA00264;P.B13GTHRSG.G3LA00264;P.B21GTHRSG.G1LA00264;P.B22GTHRSG.G2LA00264;P.B23GTHRSG.G3LA00264;P.B31GTHRSG.G1LA00264;P.B32GTHRSG.G2LA00264;P.B33GTHRSG.G3LA00264;"], "name")
        self.db_insert_if_not_exists(self.db_schema, "tbl_param", ["name", "value"], ["fuel_megawatt_value_manual", "750.0"], "name")
        self.db_insert_if_not_exists(self.db_schema, "tbl_param", ["name", "value"], ["fuel_sfc", "0.009"], "name")

        self.db_insert_if_not_exists(self.db_schema, "tbl_param", ["name", "value"], ["motor_A_bfp_ratio_threshold", "20"], "name")
        self.db_insert_if_not_exists(self.db_schema, "tbl_param", ["name", "value"], ["motor_B_bfp_ratio_threshold", "20"], "name")
        self.db_insert_if_not_exists(self.db_schema, "tbl_param", ["name", "value"], ["motor_C_bfp_ratio_threshold", "20"], "name")
        self.db_insert_if_not_exists(self.db_schema, "tbl_param", ["name", "value"], ["motor_D_bfp_ratio_threshold", "20"], "name")

        self.db_insert_if_not_exists(self.db_schema, "tbl_param", ["name", "value"], ["make_up_water_flow_threshold", "14"], "name")
        self.db_insert_if_not_exists(self.db_schema, "tbl_param", ["name", "value"], ["hp_diff_feed_water_to_steam_flow_threshold_11", "20"], "name")
        self.db_insert_if_not_exists(self.db_schema, "tbl_param", ["name", "value"], ["hp_diff_feed_water_to_steam_flow_threshold_12", "30"], "name")
        self.db_insert_if_not_exists(self.db_schema, "tbl_param", ["name", "value"], ["hp_diff_feed_water_to_steam_flow_threshold_13", "45"], "name")
        self.db_insert_if_not_exists(self.db_schema, "tbl_param", ["name", "value"], ["lp_diff_feed_water_to_steam_flow_threshold_11", "14"], "name")
        self.db_insert_if_not_exists(self.db_schema, "tbl_param", ["name", "value"], ["lp_diff_feed_water_to_steam_flow_threshold_12", "11"], "name")
        self.db_insert_if_not_exists(self.db_schema, "tbl_param", ["name", "value"], ["lp_diff_feed_water_to_steam_flow_threshold_13", "15"], "name")

        self.db_insert_if_not_exists(self.db_schema, "tbl_param", ["name", "value"], ["lp_level_uptrend_threshold_11", "0.5"], "name")
        self.db_insert_if_not_exists(self.db_schema, "tbl_param", ["name", "value"], ["lp_level_uptrend_threshold_12", "0.5"], "name")
        self.db_insert_if_not_exists(self.db_schema, "tbl_param", ["name", "value"], ["lp_level_uptrend_threshold_13", "0.5"], "name")
        self.db_insert_if_not_exists(self.db_schema, "tbl_param", ["name", "value"], ["lp_level_downtrend_threshold_11", "-1000.0"], "name")
        self.db_insert_if_not_exists(self.db_schema, "tbl_param", ["name", "value"], ["lp_level_downtrend_threshold_12", "-1000.0"], "name")
        self.db_insert_if_not_exists(self.db_schema, "tbl_param", ["name", "value"], ["lp_level_downtrend_threshold_13", "-1000.0"], "name")
        self.db_insert_if_not_exists(self.db_schema, "tbl_param", ["name", "value"], ["hp_level_uptrend_threshold_11", "0.5"], "name")
        self.db_insert_if_not_exists(self.db_schema, "tbl_param", ["name", "value"], ["hp_level_uptrend_threshold_12", "0.5"], "name")
        self.db_insert_if_not_exists(self.db_schema, "tbl_param", ["name", "value"], ["hp_level_uptrend_threshold_13", "0.5"], "name")
        self.db_insert_if_not_exists(self.db_schema, "tbl_param", ["name", "value"], ["hp_level_downtrend_threshold_11", "-1000.0"], "name")
        self.db_insert_if_not_exists(self.db_schema, "tbl_param", ["name", "value"], ["hp_level_downtrend_threshold_12", "-1000.0"], "name")
        self.db_insert_if_not_exists(self.db_schema, "tbl_param", ["name", "value"], ["hp_level_downtrend_threshold_13", "-1000.0"], "name")

        # ==============================
        # Data
        # ==============================

        tbl_data_names = ["name", "value", "timestamp"]
        tbl_data_parameters = ["VARCHAR NOT NULL", "FLOAT NOT NULL", "TIMESTAMP NOT NULL"]
        self.db_init1_table(self.db_schema, "tbl_data", tbl_data_names, tbl_data_parameters, "name")

        # ==============================
        # Fuel Param
        # ==============================

        tbl_fuel_param_names = ["name", "min_volume", "max_volume", "price"]
        tbl_fuel_param_parameters = ["VARCHAR NOT NULL PRIMARY KEY", "FLOAT NOT NULL", "FLOAT NOT NULL", "FLOAT NOT NULL"]
        self.db_create_table(self.db_schema, "tbl_fuel_param_active", tbl_fuel_param_names, tbl_fuel_param_parameters)
        self.db_create_table(self.db_schema, "tbl_fuel_param_queue", tbl_fuel_param_names, tbl_fuel_param_parameters)

        # ==============================
        # Fuel Rencana
        # ==============================

        tbl_fuel_rencana_names = ["id", "timestamp_local", "date", "sfc"]
        for i in range(48):
            tbl_fuel_rencana_names.append("mw" + str(i))
        tbl_fuel_rencana_names.append("mw_total")
        for i in range(48):
            tbl_fuel_rencana_names.append("value" + str(i))
        tbl_fuel_rencana_names.append("value_total")
        tbl_fuel_rencana_parameters = ["SERIAL NOT NULL", "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP", "DATE NOT NULL PRIMARY KEY", "FLOAT NOT NULL"]
        for i in range(48):
            tbl_fuel_rencana_parameters.append("FLOAT NOT NULL")
        tbl_fuel_rencana_parameters.append("FLOAT NOT NULL")
        for i in range(48):
            tbl_fuel_rencana_parameters.append("JSON NOT NULL")
        tbl_fuel_rencana_parameters.append("JSON NOT NULL")
        self.db_create_table(self.db_schema, "tbl_fuel_rencana", tbl_fuel_rencana_names, tbl_fuel_rencana_parameters)

        # ==============================
        # Fuel Rencana SImple
        # ==============================

        tbl_fuel_rencana_simple_names = ["id", "timestamp_local", "timestamp", "sfc", "mw", "result"]
        tbl_fuel_rencana_simple_parameters = ["SERIAL NOT NULL", "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP", "TIMESTAMP NOT NULL PRIMARY KEY", "FLOAT NOT NULL", "FLOAT NOT NULL", "JSON NOT NULL"]
        self.db_create_table(self.db_schema, "tbl_fuel_rencana_simple", tbl_fuel_rencana_simple_names, tbl_fuel_rencana_simple_parameters)

        # ==============================
        # Fuel Realisasi
        # ==============================

        tbl_fuel_realisasi_names = ["sfc", "mw", "result"]
        tbl_fuel_realisasi_parameters = ["FLOAT NOT NULL", "FLOAT NOT NULL", "JSON NOT NULL"]
        self.db_init2_table(self.db_schema, "tbl_fuel_realisasi", tbl_fuel_realisasi_names, tbl_fuel_realisasi_parameters)

        # ==============================
        # Eval Kebocoran Feed Water
        # ==============================

        tbl_eval_kebocoran_feed_water_names = ["m11_lp", "m11_hp", "m12_lp", "m12_hp", "m13_lp", "m13_hp", "leak11_lp", "leak11_hp", "leak12_lp", "leak12_hp", "leak13_lp", "leak13_hp"]
        tbl_eval_kebocoran_feed_water_parameters = ["FLOAT NOT NULL", "FLOAT NOT NULL", "FLOAT NOT NULL", "FLOAT NOT NULL", "FLOAT NOT NULL", "FLOAT NOT NULL", "INT NOT NULL", "INT NOT NULL", "INT NOT NULL", "INT NOT NULL", "INT NOT NULL", "INT NOT NULL"]
        self.db_init2_table(self.db_schema, "tbl_eval_kebocoran_feed_water", tbl_eval_kebocoran_feed_water_names, tbl_eval_kebocoran_feed_water_parameters)

        # ==============================
        # Eval ST Heat Rate
        # ==============================

        tbl_eval_st_heat_rate_names = ["blok1", "blok2", "blok3", "total"]
        tbl_eval_st_heat_rate_parameters = ["FLOAT NOT NULL", "FLOAT NOT NULL", "FLOAT NOT NULL", "FLOAT NOT NULL"]
        self.db_init2_table(self.db_schema, "tbl_eval_st_heat_rate", tbl_eval_st_heat_rate_names, tbl_eval_st_heat_rate_parameters)

        # ==============================
        # Eval Anomali BFP
        # ==============================

        tbl_eval_anomali_bfp_names = ["motor_a", "motor_b", "motor_c", "motor_d", "prediksi_rasio_motor_a", "prediksi_rasio_motor_b", "prediksi_rasio_motor_c", "prediksi_rasio_motor_d", "rasio_motor_a", "rasio_motor_b", "rasio_motor_c", "rasio_motor_d"]
        tbl_eval_anomali_bfp_parameters = ["INT NOT NULL", "INT NOT NULL", "INT NOT NULL", "INT NOT NULL", "FLOAT NOT NULL", "FLOAT NOT NULL", "FLOAT NOT NULL", "FLOAT NOT NULL", "FLOAT NOT NULL", "FLOAT NOT NULL", "FLOAT NOT NULL", "FLOAT NOT NULL"]
        self.db_init2_table(self.db_schema, "tbl_eval_anomali_bfp", tbl_eval_anomali_bfp_names, tbl_eval_anomali_bfp_parameters)

        # ==============================
        # Eval Kebocoran HRSG
        # ==============================

        tbl_eval_kebocoran_hrsg_names = ["leak_11", "leak_12", "leak_13"]
        tbl_eval_kebocoran_hrsg_parameters = ["INT NOT NULL", "INT NOT NULL", "INT NOT NULL"]
        self.db_init2_table(self.db_schema, "tbl_eval_kebocoran_hrsg", tbl_eval_kebocoran_hrsg_names, tbl_eval_kebocoran_hrsg_parameters)

        # ==============================
        # Eval Kebocoran Kondensor
        # ==============================

        tbl_eval_kebocoran_kondensor_names = ["value"]
        tbl_eval_kebocoran_kondensor_parameters = ["INT NOT NULL"]
        self.db_init2_table(self.db_schema, "tbl_eval_kebocoran_kondensor", tbl_eval_kebocoran_kondensor_names, tbl_eval_kebocoran_kondensor_parameters)

        # ==============================
        # Eval CWP
        # ==============================

        tbl_eval_cwp = ["konfigurasi_11", "konfigurasi_12", "konfigurasi_13", "rekomendasi_cwp"]
        tbl_eval_cwp_parameters = ["INT NOT NULL", "INT NOT NULL", "INT NOT NULL", "INT NOT NULL"]
        self.db_create_table(self.db_schema, "tbl_eval_cwp", tbl_eval_cwp, tbl_eval_cwp_parameters)

        # ==============================
        # Inference Output
        # ==============================

        tbl_inference_output_names = ["flame_out", "gagal_naik", "trip", "cv174_sv", "cv174_fb", "cv165_sv", "cv165_fb", "fuel_gas_flow", "cv147a_sv", "cv147a_fb", "cv147b_sv", "cv147b_fb", "cv135a_sv", "cv135a_fb", "fuel_oil_flow", "comb_shell_press", "bp_avg_temp", "log"]
        tbl_inference_output_parameters = ["INT NOT NULL", "INT NOT NULL", "INT NOT NULL", "FLOAT NOT NULL", "FLOAT NOT NULL", "FLOAT NOT NULL", "FLOAT NOT NULL", "FLOAT NOT NULL", "FLOAT NOT NULL", "FLOAT NOT NULL", "FLOAT NOT NULL", "FLOAT NOT NULL", "FLOAT NOT NULL", "FLOAT NOT NULL", "FLOAT NOT NULL", "FLOAT NOT NULL", "FLOAT NOT NULL", "VARCHAR NOT NULL"]
        self.db_init2_table(self.db_schema, "tbl_inference_output_11", tbl_inference_output_names, tbl_inference_output_parameters)
        self.db_init2_table(self.db_schema, "tbl_inference_output_12", tbl_inference_output_names, tbl_inference_output_parameters)
        self.db_init2_table(self.db_schema, "tbl_inference_output_13", tbl_inference_output_names, tbl_inference_output_parameters)

        # ==============================
        # Status Logic Nodes
        # ==============================

        tbl_status_logic_nodes_names = ["name", "value"]
        tbl_status_logic_nodes_parameters = ["VARCHAR NOT NULL PRIMARY KEY", "INT NOT NULL"]
        self.db_create_table(self.db_schema, "tbl_status_logic_nodes", tbl_status_logic_nodes_names, tbl_status_logic_nodes_parameters)

        # ==============================
        # Status Report Read
        # ==============================

        tbl_status_report_read_names = ["name", "value"]
        tbl_status_report_read_parameters = ["VARCHAR NOT NULL PRIMARY KEY", "INT NOT NULL"]
        self.db_create_table(self.db_schema, "tbl_status_report_read", tbl_status_report_read_names, tbl_status_report_read_parameters)

        # ==============================
        # Users
        # ==============================

        tbl_users_names = ["username", "password", "role", "last_login"]
        tbl_users_parameters = ["VARCHAR NOT NULL PRIMARY KEY", "VARCHAR NOT NULL", "VARCHAR NOT NULL", "TIMESTAMPZ NOT NULL"]
        self.db_create_table(self.db_schema, "tbl_users", tbl_users_names, tbl_users_parameters)

        return 0

    # --------------------------------------------------------------------------

    def db_create_table(self, table_schema, table_name, column_names, column_parameters):
        if not self.is_create_table:
            return

        is_exist = False
        is_same = False

        sql = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '" + table_schema + "' AND table_name = '" + table_name + "')"
        self.mutex_db.acquire()
        self.myCursor.execute(sql)
        response = self.myCursor.fetchall()
        self.myDatabase.commit()
        self.mutex_db.release()
        if response[0][0]:
            is_exist = True

        if is_exist:
            sql = "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '" + table_schema + "' AND table_name = '" + table_name + "'"
            self.mutex_db.acquire()
            self.myCursor.execute(sql)
            response = self.myCursor.fetchall()
            self.myDatabase.commit()
            self.mutex_db.release()
            if len(response) == len(column_names):
                is_same = True
                for i in range(len(response)):
                    if response[i][0] != column_names[i]:
                        is_same = False

        if is_exist and not is_same:
            sql = "DROP TABLE " + table_schema + "." + table_name
            self.mutex_db.acquire()
            self.myCursor.execute(sql)
            self.myDatabase.commit()
            self.mutex_db.release()
            is_exist = False

        if not is_exist:
            sql = "CREATE TABLE IF NOT EXISTS " + table_schema + "." + table_name + " ("
            for i in range(len(column_names)):
                sql += column_names[i] + " " + column_parameters[i]
                if i != len(column_names) - 1:
                    sql += ", "
            sql += ")"
            self.mutex_db.acquire()
            self.myCursor.execute(sql)
            self.myDatabase.commit()
            self.mutex_db.release()

    def db_insert(self, table_schema, table_name, columns, values):
        sql = "INSERT INTO " + table_schema + "." + table_name + " ("
        for i in range(len(columns)):
            sql += columns[i]
            if i != len(columns) - 1:
                sql += ", "
        sql += ") VALUES ("
        for i in range(len(values)):
            sql += "'" + str(values[i]) + "'"
            if i != len(values) - 1:
                sql += ", "
        sql += ")"
        self.mutex_db.acquire()
        self.myCursor.execute(sql)
        self.myDatabase.commit()
        self.mutex_db.release()

    def db_select(self, table_schema, table_name, columns, where):
        sql = "SELECT "
        for i in range(len(columns)):
            sql += columns[i]
            if i != len(columns) - 1:
                sql += ", "
        sql += " FROM " + table_schema + "." + table_name
        if where != "":
            sql += " WHERE " + where
        self.mutex_db.acquire()
        self.myCursor.execute(sql)
        response = self.myCursor.fetchall()
        self.myDatabase.commit()
        self.mutex_db.release()

        if columns == ["*"]:
            sql = "SELECT column_name FROM information_schema.columns WHERE table_schema = '" + table_schema + "' AND table_name = '" + table_name + "'"
            self.mutex_db.acquire()
            self.myCursor.execute(sql)
            response_column_names = self.myCursor.fetchall()
            self.myDatabase.commit()
            self.mutex_db.release()

            column_names = []
            for i in range(len(response_column_names)):
                column_names.append(response_column_names[i][0])

            return pd.DataFrame(response, columns=column_names).to_json(orient="split", indent=2)

        return pd.DataFrame(response, columns=columns).to_json(orient="split", indent=2)

    def db_update(self, table_schema, table_name, columns, values, where):
        sql = "UPDATE " + table_schema + "." + table_name + " SET "
        for i in range(len(columns)):
            sql += columns[i] + " = '" + str(values[i]) + "'"
            if i != len(columns) - 1:
                sql += ", "
        if where != "":
            sql += " WHERE " + where
        self.mutex_db.acquire()
        self.myCursor.execute(sql)
        self.myDatabase.commit()
        self.mutex_db.release()

    def db_upsert(self, table_schema, table_name, columns, values, primary_key):
        sql = "INSERT INTO " + table_schema + "." + table_name + " ("
        for i in range(len(columns)):
            sql += columns[i]
            if i != len(columns) - 1:
                sql += ", "
        sql += ") VALUES ("
        for i in range(len(values)):
            sql += "'" + str(values[i]) + "'"
            if i != len(values) - 1:
                sql += ", "
        sql += ") ON CONFLICT (" + primary_key + ") DO UPDATE SET "
        for i in range(len(columns)):
            sql += columns[i] + " = '" + str(values[i]) + "'"
            if i != len(columns) - 1:
                sql += ", "
        self.mutex_db.acquire()
        self.myCursor.execute(sql)
        self.myDatabase.commit()
        self.mutex_db.release()

    def db_insert_if_not_exists(self, table_schema, table_name, columns, values, primary_key):
        sql = "INSERT INTO " + table_schema + "." + table_name + " ("
        for i in range(len(columns)):
            sql += columns[i]
            if i != len(columns) - 1:
                sql += ", "
        sql += ") VALUES ("
        for i in range(len(values)):
            sql += "'" + str(values[i]) + "'"
            if i != len(values) - 1:
                sql += ", "
        sql += ") ON CONFLICT (" + primary_key + ") DO NOTHING;"
        self.mutex_db.acquire()
        self.myCursor.execute(sql)
        self.myDatabase.commit()
        self.mutex_db.release()

    def db_delete(self, table_schema, table_name, where):
        sql = "DELETE FROM " + table_schema + "." + table_name
        if where != "":
            sql += " WHERE " + where
        self.mutex_db.acquire()
        self.myCursor.execute(sql)
        self.myDatabase.commit()
        self.mutex_db.release()

    # --------------------------------------------------------------------------

    def db_init1_table(self, table_schema, table_name, column_names, column_parameters, primary_key):
        if not self.is_create_table:
            return

        # Create table
        _column_names = ['id', 'timestamp_local']
        _column_parameters = ['SERIAL NOT NULL PRIMARY KEY', 'TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP']
        self.db_create_table(table_schema, table_name, _column_names + column_names, _column_parameters + column_parameters)
        self.db_create_table(table_schema, table_name + "_last1800sec", _column_names + column_names, _column_parameters + column_parameters)

        _column_names = ['id', 'timestamp_local']
        _column_parameters = ['SERIAL NOT NULL', 'TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP']
        self.db_create_table(table_schema, table_name + "_last", _column_names + column_names, _column_parameters + column_parameters)

        try:
            # Add primary key to "_last" table
            sql = "ALTER TABLE " + table_schema + "." + table_name + "_last ADD PRIMARY KEY (" + primary_key + ");"
            self.mutex_db.acquire()
            self.myCursor.execute(sql)
            self.myDatabase.commit()
            self.mutex_db.release()
        except BaseException as e:
            pass

        # ------------------------------

        # Create function to insert into "_last1800sec" and upsert into "_last" table
        sql = "CREATE OR REPLACE FUNCTION " + table_name + "_insert() RETURNS TRIGGER AS $$ BEGIN "
        sql += "INSERT INTO " + table_schema + "." + table_name + "_last1800sec ("
        for i in range(len(column_names)):
            sql += column_names[i]
            if i != len(column_names) - 1:
                sql += ", "
        sql += ") VALUES ("
        for i in range(len(column_names)):
            sql += "NEW." + column_names[i]
            if i != len(column_names) - 1:
                sql += ", "
        sql += "); "
        sql += "INSERT INTO " + table_schema + "." + table_name + "_last ("
        for i in range(len(column_names)):
            sql += column_names[i]
            if i != len(column_names) - 1:
                sql += ", "
        sql += ") VALUES ("
        for i in range(len(column_names)):
            sql += "NEW." + column_names[i]
            if i != len(column_names) - 1:
                sql += ", "
        sql += ") ON CONFLICT (" + primary_key + ") DO UPDATE SET "
        for i in range(len(column_names)):
            sql += column_names[i] + " = NEW." + column_names[i]
            if i != len(column_names) - 1:
                sql += ", "
        sql += "; "
        sql += "RETURN NEW; END; $$ LANGUAGE plpgsql;"
        self.mutex_db.acquire()
        self.myCursor.execute(sql)
        self.myDatabase.commit()
        self.mutex_db.release()

        # Create trigger to insert into "_last1800sec" and upsert into "_last" table
        sql = "CREATE OR REPLACE TRIGGER " + table_name + "_insert AFTER INSERT ON " + table_schema + "." + table_name + " FOR EACH ROW EXECUTE PROCEDURE " + table_name + "_insert();"
        self.mutex_db.acquire()
        self.myCursor.execute(sql)
        self.myDatabase.commit()
        self.mutex_db.release()

        # ------------------------------

        # Create function to delete from "_last1800sec" table
        sql = "CREATE OR REPLACE FUNCTION " + table_name + "_delete_1800sec() RETURNS TRIGGER AS $$ BEGIN "
        sql += "DELETE FROM " + table_schema + "." + table_name + "_last1800sec WHERE timestamp_local < NOW() - INTERVAL '1800 seconds'; "
        sql += "RETURN NEW; END; $$ LANGUAGE plpgsql;"
        self.mutex_db.acquire()
        self.myCursor.execute(sql)
        self.myDatabase.commit()
        self.mutex_db.release()

        # Create trigger to delete from "_last1800sec" table
        sql = "CREATE OR REPLACE TRIGGER " + table_name + "_delete_1800sec BEFORE INSERT ON " + table_schema + "." + table_name + "_last1800sec FOR EACH ROW EXECUTE PROCEDURE " + table_name + "_delete_1800sec();"
        self.mutex_db.acquire()
        self.myCursor.execute(sql)
        self.myDatabase.commit()
        self.mutex_db.release()

    def db_init2_table(self, table_schema, table_name, column_names, column_parameters):
        if not self.is_create_table:
            return

        _column_names = ['id', 'timestamp_local']
        _column_parameters = ['SERIAL NOT NULL PRIMARY KEY', 'TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP']

        # Create table
        self.db_create_table(table_schema, table_name, _column_names + column_names, _column_parameters + column_parameters)
        self.db_create_table(table_schema, table_name + "_last1800sec", _column_names + column_names, _column_parameters + column_parameters)
        self.db_create_table(table_schema, table_name + "_last", _column_names + column_names, _column_parameters + column_parameters)

        # ------------------------------

        # Create function to insert into "_last1800sec" and "_last" table
        sql = "CREATE OR REPLACE FUNCTION " + table_name + "_insert() RETURNS TRIGGER AS $$ BEGIN "
        sql += "INSERT INTO " + table_schema + "." + table_name + "_last1800sec ("
        for i in range(len(column_names)):
            sql += column_names[i]
            if i != len(column_names) - 1:
                sql += ", "
        sql += ") VALUES ("
        for i in range(len(column_names)):
            sql += "NEW." + column_names[i]
            if i != len(column_names) - 1:
                sql += ", "
        sql += "); "
        sql += "INSERT INTO " + table_schema + "." + table_name + "_last ("
        for i in range(len(column_names)):
            sql += column_names[i]
            if i != len(column_names) - 1:
                sql += ", "
        sql += ") VALUES ("
        for i in range(len(column_names)):
            sql += "NEW." + column_names[i]
            if i != len(column_names) - 1:
                sql += ", "
        sql += "); "
        sql += "RETURN NEW; END; $$ LANGUAGE plpgsql;"
        self.mutex_db.acquire()
        self.myCursor.execute(sql)
        self.myDatabase.commit()
        self.mutex_db.release()

        # Create trigger to insert into "_last1800sec" and "_last" table
        sql = "CREATE OR REPLACE TRIGGER " + table_name + "_insert AFTER INSERT ON " + table_schema + "." + table_name + " FOR EACH ROW EXECUTE PROCEDURE " + table_name + "_insert();"
        self.mutex_db.acquire()
        self.myCursor.execute(sql)
        self.myDatabase.commit()
        self.mutex_db.release()

        # ------------------------------

        # Create function to delete from "_last1800sec" table
        sql = "CREATE OR REPLACE FUNCTION " + table_name + "_delete_1800sec() RETURNS TRIGGER AS $$ BEGIN "
        sql += "DELETE FROM " + table_schema + "." + table_name + "_last1800sec WHERE timestamp_local < NOW() - INTERVAL '1800 seconds'; "
        sql += "RETURN NEW; END; $$ LANGUAGE plpgsql;"
        self.mutex_db.acquire()
        self.myCursor.execute(sql)
        self.myDatabase.commit()
        self.mutex_db.release()

        # Create trigger to delete from "_last1800sec" table
        sql = "CREATE OR REPLACE TRIGGER " + table_name + "_delete_1800sec BEFORE INSERT ON " + table_schema + "." + table_name + "_last1800sec FOR EACH ROW EXECUTE PROCEDURE " + table_name + "_delete_1800sec();"
        self.mutex_db.acquire()
        self.myCursor.execute(sql)
        self.myDatabase.commit()
        self.mutex_db.release()

        # ------------------------------

        # Create function to delete from "_last" table
        sql = "CREATE OR REPLACE FUNCTION " + table_name + "_delete_last() RETURNS TRIGGER AS $$ BEGIN "
        sql += "DELETE FROM " + table_schema + "." + table_name + "_last; "
        sql += "RETURN NEW; END; $$ LANGUAGE plpgsql;"
        self.mutex_db.acquire()
        self.myCursor.execute(sql)
        self.myDatabase.commit()
        self.mutex_db.release()

        # Create trigger to delete from "_last" table
        sql = "CREATE OR REPLACE TRIGGER " + table_name + "_delete_last BEFORE INSERT ON " + table_schema + "." + table_name + "_last FOR EACH ROW EXECUTE PROCEDURE " + table_name + "_delete_last();"
        self.mutex_db.acquire()
        self.myCursor.execute(sql)
        self.myDatabase.commit()
        self.mutex_db.release()


if __name__ == "__main__":
    is_create_table = False
    for i in range(len(sys.argv)):
        if sys.argv[i] == "--is_create_table":
            is_create_table = True
            break

    rospy.init_node("interface_database")
    interface_database = InterfaceDatabase(is_create_table)
    rospy.spin()
