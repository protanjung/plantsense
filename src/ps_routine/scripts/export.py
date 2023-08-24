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
import threading
import pandas as pd
import numpy as np
from flask import Flask, request, jsonify
from flask_cors import CORS


class Export():
    def __init__(self):
        # =====Parameter
        self.export_path = rospy.get_param("export_path", os.path.expanduser("~"))
        # =====ServiceClient
        self.cli_db_insert = rospy.ServiceProxy("db_insert", db_insert)
        self.cli_db_select = rospy.ServiceProxy("db_select", db_select)
        self.cli_db_update = rospy.ServiceProxy("db_update", db_update)
        self.cli_db_upsert = rospy.ServiceProxy("db_upsert", db_upsert)
        self.cli_db_delete = rospy.ServiceProxy("db_delete", db_delete)

        if self.export_init() == -1:
            rospy.signal_shutdown("")

    # --------------------------------------------------------------------------

    def export_init(self):
        time.sleep(2.0)

        # Flask
        flask_thread = threading.Thread(target=self.thread_flask)
        flask_thread.daemon = True
        flask_thread.start()

        # If export path does not exist, create it
        if not os.path.exists(self.export_path):
            try:
                os.makedirs(self.export_path)
            except Exception as e:
                rospy.logerr("Error: " + str(e))
                return -1

        return 0

    # --------------------------------------------------------------------------

    def get_data_step_1(self, tags, time_start, time_stop):
        '''
        Get data from database (step 1)
        :param tags: list of tags
        :param time_start: start time
        :param time_stop: stop time
        :return: pandas dataframe with timestamp as index, tags as columns, and values as values
        '''

        # Safety: if time_start is greater than time_stop, return empty dataframe
        if time_start > time_stop:
            rospy.logerr("Error: time_start is greater than time_stop")
            return pd.DataFrame()
        # Safety: if time_start is equal to time_stop, return empty dataframe
        if time_start == time_stop:
            rospy.logerr("Error: time_start is equal to time_stop")
            return pd.DataFrame()

        # Construct where clause
        where = "timestamp >= '" + time_start + "' and timestamp <= '" + time_stop + "'" + " and name in ("
        for i in range(len(tags)):
            where += "'" + tags[i] + "'"
            if i != len(tags) - 1:
                where += ","
        where += ")"

        # Get data from database
        ret_json = self.cli_db_select("tbl_data", ["*"], where)
        ret_df = pd.read_json(ret_json.response, orient="split")

        try:
            # Copy data
            temp = ret_df.copy()
            # Pivot data to get timestamp as index, tags as columns, and values as values using mean as aggregator
            temp = temp.pivot_table(index="timestamp", columns="name", values="value", aggfunc=np.mean)
            # Return data
            ret_df = temp.copy()
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            rospy.logerr("Error: " + str(e) + " at line " + str(exc_tb.tb_lineno))
            return pd.DataFrame()

        return ret_df

    def get_data_step_2(self, df, time_start, time_stop, period):
        '''
        Get data from database (step 2)
        :param df: pandas dataframe with timestamp as index, tags as columns, and values as values
        :param time_start: start time
        :param time_stop: stop time
        :return: modified pandas dataframe with complete and ordered timestamp
        '''

        # Safety: if dataframe is empty, return empty dataframe
        if df.empty:
            rospy.logerr("Error: dataframe is empty")
            return pd.DataFrame()

        try:
            # Copy data
            temp = df.copy()
            # Add Nan value at time_start and time_stop
            temp.loc[pd.Timestamp(time_start)] = None
            temp.loc[pd.Timestamp(time_stop)] = None
            # Resample data to get data at every period
            temp = temp.resample(str(period) + "S").last()
            # Return data
            ret_df = temp.copy()
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            rospy.logerr("Error: " + str(e) + " at line " + str(exc_tb.tb_lineno))
            return pd.DataFrame()

        return ret_df

    def get_data_step_3(self, df):
        '''
        Get data from database (step 3)
        :param df: pandas dataframe with timestamp as index, tags as columns, and values as values
        :return: modified pandas dataframe with interpolated values
        '''

        # Safety: if dataframe is empty, return empty dataframe
        if df.empty:
            rospy.logerr("Error: dataframe is empty")
            return pd.DataFrame()

        try:
            # Copy data
            temp = df.copy()
            # Replace NaN value with previous value
            temp = temp.fillna(method="ffill")
            # Round value to 3 decimal places
            temp = temp.round(3)
            # Return data
            ret_df = temp.copy()
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            rospy.logerr("Error: " + str(e) + " at line " + str(exc_tb.tb_lineno))
            return pd.DataFrame()

        return ret_df

    # --------------------------------------------------------------------------

    def export_data(self, tags, time_start, time_stop, period):
        export_name = time.strftime("%Y%m%d%H%M%S") + ".csv"
        export_path = os.path.join(self.export_path, export_name)

        # Mark time start
        export_start = time.time()

        try:
            # Going through step 1, 2, and 3
            step1 = self.get_data_step_1(tags, time_start, time_stop)
            step2 = self.get_data_step_2(step1, time_start, time_stop, period)
            step3 = self.get_data_step_3(step2)
            # Export data to csv
            step3.to_csv(export_path)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            return {"status": "-1", "message": str(e) + " at line " + str(exc_tb.tb_lineno)}

        # Mark time stop
        export_stop = time.time()
        # Calculate elapsed time
        elapsed_time = export_stop - export_start

        return {"status": "0", "message": "Berhasil", "path": export_path, "name": export_name, "time": elapsed_time}

    # --------------------------------------------------------------------------

    app = Flask(__name__)
    CORS(app)

    @app.route("/export", methods=["POST"])
    def flask_export():
        param = {}
        param["tags"] = request.form.getlist("tags") if "tags" in request.form else []
        if "time_start" in request.form:
            param["time_start"] = request.form["time_start"]
        elif "time_begin" in request.form:
            param["time_start"] = request.form["time_begin"]
        else:
            param["time_start"] = time.strftime("%Y-%m-%d %H:%M:%S")
        if "time_stop" in request.form:
            param["time_stop"] = request.form["time_stop"]
        elif "time_end" in request.form:
            param["time_stop"] = request.form["time_end"]
        else:
            param["time_stop"] = time.strftime("%Y-%m-%d %H:%M:%S")
        param["period"] = request.form["period"] if "period" in request.form else 1

        return export.export_data(param["tags"], param["time_start"], param["time_stop"], param["period"])

    def thread_flask(self):
        self.app.run(host="0.0.0.0", port=5001)


if __name__ == "__main__":
    rospy.init_node("export", anonymous=False)
    export = Export()
    rospy.spin()
