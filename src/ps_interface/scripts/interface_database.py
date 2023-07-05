#!/usr/bin/python

import rospy
from ps_interface.srv import db_insert, db_insertResponse
from ps_interface.srv import db_select, db_selectResponse
from ps_interface.srv import db_update, db_updateResponse
from ps_interface.srv import db_upsert, db_upsertResponse
from ps_interface.srv import db_delete, db_deleteResponse
import psycopg2
import pandas as pd


class InterfaceDatabase():
    def __init__(self):
        # =====Parameter
        self.db_host = rospy.get_param("db/host", "localhost")
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
        self.db_insert(self.db_schema, req.table_name, req.columns, req.values)

        res = db_insertResponse()
        return res

    def cllbck_srv_db_select(self, req):
        response = self.db_select(self.db_schema, req.table_name, req.columns, req.where)

        res = db_selectResponse()
        res.response = response
        return res

    def cllbck_srv_db_update(self, req):
        self.db_update(self.db_schema, req.table_name, req.columns, req.values, req.where)

        res = db_updateResponse()
        return res

    def cllbck_srv_db_upsert(self, req):
        self.db_upsert(self.db_schema, req.table_name, req.columns, req.values, req.primary_key)

        res = db_upsertResponse()
        return res

    def cllbck_srv_db_delete(self, req):
        self.db_delete(self.db_schema, req.table_name, req.where)

        res = db_deleteResponse()
        return res

    # --------------------------------------------------------------------------

    def interface_database_init(self):
        self.myDatabase = psycopg2.connect(
            host=self.db_host,
            port=self.db_port,
            user=self.db_user,
            password=self.db_password,
            database=self.db_database
        )
        self.myCursor = self.myDatabase.cursor()

        tbl_param_names = ["name", "value"]
        tbl_param_parameters = ["VARCHAR NOT NULL PRIMARY KEY", "VARCHAR NOT NULL"]
        self.db_create_table(self.db_schema, "tbl_param", tbl_param_names, tbl_param_parameters)

        tbl_fuel_param_names = ["name", "min_volume", "max_volume", "price"]
        tbl_fuel_param_parameters = ["VARCHAR NOT NULL PRIMARY KEY", "FLOAT NOT NULL", "FLOAT NOT NULL", "FLOAT NOT NULL"]
        self.db_create_table(self.db_schema, "tbl_fuel_param", tbl_fuel_param_names, tbl_fuel_param_parameters)

        tbl_fuel_optimization_names = ["id", "result", "timestamp_local"]
        tbl_fuel_optimization_parameters = ["SERIAL NOT NULL PRIMARY KEY", "VARCHAR NOT NULL", "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"]
        self.db_create_table(self.db_schema, "tbl_fuel", tbl_fuel_optimization_names, tbl_fuel_optimization_parameters)
        self.db_create_table(self.db_schema, "tbl_fuel_last", tbl_fuel_optimization_names, tbl_fuel_optimization_parameters)
        self.db_create_table(self.db_schema, "tbl_fuel_per60sec", tbl_fuel_optimization_names, tbl_fuel_optimization_parameters)
        self.db_create_table(self.db_schema, "tbl_fuel_per1800sec", tbl_fuel_optimization_names, tbl_fuel_optimization_parameters)

        tbl_data_names = ["id", "name", "value", "timestamp", "timestamp_local"]
        tbl_data_parameters = ["SERIAL NOT NULL PRIMARY KEY", "VARCHAR NOT NULL", "FLOAT NOT NULL", "TIMESTAMP NOT NULL", "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"]
        self.db_create_table(self.db_schema, "tbl_data", tbl_data_names, tbl_data_parameters)
        self.db_create_table(self.db_schema, "tbl_data_last60sec", tbl_data_names, tbl_data_parameters)
        self.db_create_table(self.db_schema, "tbl_data_last1800sec", tbl_data_names, tbl_data_parameters)
        tbl_data_names = ["name", "value", "timestamp"]
        tbl_data_parameters = ["VARCHAR NOT NULL PRIMARY KEY", "FLOAT NOT NULL", "TIMESTAMP NOT NULL"]
        self.db_create_table(self.db_schema, "tbl_data_last", tbl_data_names, tbl_data_parameters)

        return 0

    # --------------------------------------------------------------------------

    def db_create_table(self, table_schema, table_name, column_names, column_parameters):
        sql = "CREATE TABLE IF NOT EXISTS " + table_schema + "." + table_name + " ("
        for i in range(len(column_names)):
            sql += column_names[i] + " " + column_parameters[i]
            if i != len(column_names) - 1:
                sql += ", "
        sql += ")"
        self.myCursor.execute(sql)
        self.myDatabase.commit()

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
        self.myCursor.execute(sql)
        self.myDatabase.commit()

    def db_select(self, table_schema, table_name, columns, where):
        sql = "SELECT "
        for i in range(len(columns)):
            sql += columns[i]
            if i != len(columns) - 1:
                sql += ", "
        sql += " FROM " + table_schema + "." + table_name
        if where != "":
            sql += " WHERE " + where
        self.myCursor.execute(sql)
        response = self.myCursor.fetchall()
        self.myDatabase.commit()
        return pd.DataFrame(response, columns=columns).to_json(orient="split", indent=2)

    def db_update(self, table_schema, table_name, columns, values, where):
        sql = "UPDATE " + table_schema + "." + table_name + " SET "
        for i in range(len(columns)):
            sql += columns[i] + " = '" + str(values[i]) + "'"
            if i != len(columns) - 1:
                sql += ", "
        if where != "":
            sql += " WHERE " + where
        self.myCursor.execute(sql)
        self.myDatabase.commit()

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
        self.myCursor.execute(sql)
        self.myDatabase.commit()

    def db_delete(self, table_schema, table_name, where):
        sql = "DELETE FROM " + table_schema + "." + table_name
        if where != "":
            sql += " WHERE " + where
        self.myCursor.execute(sql)
        self.myDatabase.commit()


if __name__ == "__main__":
    rospy.init_node("interface_database")
    interface_database = InterfaceDatabase()
    rospy.spin()
