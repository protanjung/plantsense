#!/usr/bin/python3

import rospy
from ps_interface.srv import db_insert, db_insertResponse
from ps_interface.srv import db_select, db_selectResponse
from ps_interface.srv import db_update, db_updateResponse
from ps_interface.srv import db_upsert, db_upsertResponse
from ps_interface.srv import db_delete, db_deleteResponse
import time
import pandas as pd
import numpy as np


class Evaluation:
    def __init__(self):
        # =====Timer
        self.tim_2hz = rospy.Timer(rospy.Duration(0.5), self.cllbck_tim_2hz)
        # =====ServiceClient
        self.cli_db_insert = rospy.ServiceProxy("db_insert", db_insert)
        self.cli_db_select = rospy.ServiceProxy("db_select", db_select)
        self.cli_db_update = rospy.ServiceProxy("db_update", db_update)
        self.cli_db_upsert = rospy.ServiceProxy("db_upsert", db_upsert)
        self.cli_db_delete = rospy.ServiceProxy("db_delete", db_delete)

        self.isFirst10Second = True
        self.last10Second = 0

        if self.evaluation_init() == -1:
            rospy.signal_shutdown("")

    # --------------------------------------------------------------------------

    def cllbck_tim_2hz(self, event):
        if time.localtime().tm_sec % 10 != 0:
            return

        if self.last10Second == time.localtime().tm_sec:
            return

        self.last10Second = time.localtime().tm_sec

        # ----------

        self.kebocoran_feed_water()

    # --------------------------------------------------------------------------

    def evaluation_init(self):
        time.sleep(2.0)

        return 0

    # --------------------------------------------------------------------------

    def get_raw_data_window(self, name, window, period):
        '''
        Get raw data from database
        :param name: list of OPC tags
        :param window: time window in seconds, it must be between 0 and 1800
        :param period: time period in seconds, it must be divisible by window
        :return: pandas dataframe with timestamp as index, OPC tags as columns, and OPC values as values
        '''
        if window <= 0 or window >= 1800:
            rospy.logerr("Error: window must be between 0 and 1800")
            return pd.DataFrame()
        if window % period != 0:
            rospy.logerr("Error: window must be divisible by period")
            return pd.DataFrame()

        # Construct where clause
        where = "timestamp_local > now() - interval '" + str(window) + " second' and name in ("
        for i in range(len(name)):
            where += "'" + name[i] + "'"
            if i < len(name) - 1:
                where += ", "
        where += ")"

        # Get data from database
        ret_json = self.cli_db_select("tbl_data_last1800sec", ["*"], where)
        ret_df = pd.read_json(ret_json.response, orient="split")

        # Set time stop and time start
        time_stop = pd.Timestamp.now()
        time_start = time_stop - pd.Timedelta(seconds=window)

        try:
            # Copy data
            temp = ret_df.copy()
            # Pivot data to get timestamp as index, OPC tags as columns, and OPC values as values
            temp = temp.pivot(index="timestamp_local", columns="name", values="value")
            # Remove data outside time window
            temp = temp.loc[time_start:time_stop]
            # Add NaN value at time start and time stop
            temp.loc[time_start] = None
            temp.loc[time_stop] = None
            # Sort data by timestamp index in ascending order
            temp = temp.sort_index()
            # Resample data to get data at every period
            temp = temp.resample(str(period) + "S").mean()
            # Interpolate data to fill NaN value
            temp = temp.interpolate(method="linear", limit_direction="both")
            # Return data
            ret_df = temp.copy()
        except BaseException as e:
            rospy.logerr("Error: " + str(e))
            return pd.DataFrame()

        return ret_df

    def get_median_per_row(self, df):
        '''
        Get median value per row
        :param df: pandas dataframe with timestamp as index, OPC tags as columns, and OPC values as values
        :return: pandas dataframe with timestamp as index, and median value as values
        '''
        if df.empty:
            rospy.logerr("Error: df must have at least 1 row")
            return pd.DataFrame()

        return df.median(axis=1).to_frame("median")

    def get_median_per_column(self, df):
        '''
        Get median value per column
        :param df: pandas dataframe with timestamp as index, OPC tags as columns, and OPC values as values
        :return: pandas dataframe with OPC tags as index, and median value as values
        '''
        if df.empty:
            rospy.logerr("Error: df must have at least 1 column")
            return pd.DataFrame()

        return df.median(axis=0).to_frame("median")

    def get_average_per_row(self, df):
        '''
        Get average value per row
        :param df: pandas dataframe with timestamp as index, OPC tags as columns, and OPC values as values
        :return: pandas dataframe with timestamp as index, and average value as values
        '''
        if df.empty:
            rospy.logerr("Error: df must have at least 1 row")
            return pd.DataFrame()

        return df.mean(axis=1).to_frame("average")

    def get_average_per_column(self, df):
        '''
        Get average value per column
        :param df: pandas dataframe with timestamp as index, OPC tags as columns, and OPC values as values
        :return: pandas dataframe with OPC tags as index, and average value as values
        '''
        if df.empty:
            rospy.logerr("Error: df must have at least 1 column")
            return pd.DataFrame()

        return df.mean(axis=0).to_frame("average")

    def linear_regression(self, df):
        '''
        Perform linear regression on a pandas dataframe with only one column.
        :param df: pandas dataframe with only one column
        :return: tuple with slope and y-intercept of the linear regression model
        '''
        if df.shape[0] < 2:
            rospy.logerr("Error: df must have at least 2 row")
            return pd.DataFrame()
        if df.shape[1] != 1:
            rospy.logerr("Error: df must have only 1 column")
            return pd.DataFrame()

        x = np.arange(df.shape[0])  # x = [0, 1, 2, ..., n]
        y = df.iloc[:, 0].values    # y = [y0, y1, y2, ..., yn]
        m, c = np.polyfit(x, y, 1)  # model = [m, c] where y = mx + c

        return m, c

    # --------------------------------------------------------------------------

    def kebocoran_feed_water(self):
        tag_damper_full_open = [["P.B11GTHRSG.H1DI00068"],
                                ["P.B12GTHRSG.H2DI00068"],
                                ["P.B13GTHRSG.H3DI00068"]]
        tag_damper_full_close = [["P.B11GTHRSG.H1DI00069"],
                                 ["P.B12GTHRSG.H2DI00069"],
                                 ["P.B13GTHRSG.H3DI00069"]]

        isopen_damper = []
        isclose_damper = []

        try:
            for i in range(3):
                raw_damper_full_open = self.get_raw_data_window(tag_damper_full_open[i], 10, 10)
                raw_damper_full_close = self.get_raw_data_window(tag_damper_full_close[i], 10, 10)
                average_damper_full_open = self.get_average_per_column(raw_damper_full_open).iloc[0, 0]
                average_damper_full_close = self.get_average_per_column(raw_damper_full_close).iloc[0, 0]
                isopen_damper += [True if average_damper_full_open > 0.99 else False]
                isclose_damper += [True if average_damper_full_close > 0.99 else False]
        except BaseException as e:
            rospy.logerr("Error: " + str(e))
            return

        # ==============================

        tag_cso_lcv_lp = [["P.B11GTHRSG.H1AO00004"],
                          ["P.B12GTHRSG.H2AO00004"],
                          ["P.B13GTHRSG.H3AO00004"]]
        tag_cso_lcv_hp = [["P.B11GTHRSG.H1AO00005"],
                          ["P.B12GTHRSG.H2AO00005"],
                          ["P.B13GTHRSG.H3AO00005"]]

        isclose_cso_lcv_lp = []
        isclose_cso_lcv_hp = []

        try:
            for i in range(3):
                raw_cso_lcv_lp = self.get_raw_data_window(tag_cso_lcv_lp[i], 10, 10)
                raw_cso_lcv_hp = self.get_raw_data_window(tag_cso_lcv_hp[i], 10, 10)
                average_cso_lcv_lp = self.get_average_per_column(raw_cso_lcv_lp).iloc[0, 0]
                average_cso_lcv_hp = self.get_average_per_column(raw_cso_lcv_hp).iloc[0, 0]
                isclose_cso_lcv_lp += [1 if average_cso_lcv_lp > 99.99 else 0]
                isclose_cso_lcv_hp += [1 if average_cso_lcv_hp > 99.99 else 0]
        except BaseException as e:
            rospy.logerr("Error: " + str(e))
            return

        # ==============================

        lp_level_uptrend_threshold = float(rospy.get_param("lp_level_uptrend_threshold", 0.1))
        lp_level_downtrend_threshold = float(rospy.get_param("lp_level_downtrend_threshold", -0.1))
        hp_level_uptrend_threshold = float(rospy.get_param("hp_level_uptrend_threshold", 0.1))
        hp_level_downtrend_threshold = float(rospy.get_param("hp_level_downtrend_threshold", -0.1))

        tag_lp_level = [["P.B11GTHRSG.H1LA00274", "P.B11GTHRSG.H1LA00275", "P.B11GTHRSG.H1LA00276"],
                        ["P.B12GTHRSG.H2LA00274", "P.B12GTHRSG.H2LA00275", "P.B12GTHRSG.H2LA00276"],
                        ["P.B13GTHRSG.H3LA00274", "P.B13GTHRSG.H3LA00275", "P.B13GTHRSG.H3LA00276"]]
        tag_hp_level = [["P.B11GTHRSG.H1LA00277", "P.B11GTHRSG.H1LA00278", "P.B11GTHRSG.H1LA00279"],
                        ["P.B12GTHRSG.H2LA00277", "P.B12GTHRSG.H2LA00278", "P.B12GTHRSG.H2LA00279"],
                        ["P.B13GTHRSG.H3LA00277", "P.B13GTHRSG.H3LA00278", "P.B13GTHRSG.H3LA00279"]]

        m_c_lp_level = []
        m_c_hp_level = []
        trend_lp_level = []
        trend_hp_level = []

        try:
            for i in range(3):
                raw_lp_level = self.get_raw_data_window(tag_lp_level[i], 600, 10)
                raw_hp_level = self.get_raw_data_window(tag_hp_level[i], 600, 10)
                median_lp_level = self.get_median_per_row(raw_lp_level)
                median_hp_level = self.get_median_per_row(raw_hp_level)
                m_c_lp_level += [self.linear_regression(median_lp_level)]
                m_c_hp_level += [self.linear_regression(median_hp_level)]
                trend_lp_level += [1 if m_c_lp_level[i][0] > lp_level_uptrend_threshold else -1 if m_c_lp_level[i][0] < lp_level_downtrend_threshold else 0]
                trend_hp_level += [1 if m_c_hp_level[i][0] > hp_level_uptrend_threshold else -1 if m_c_hp_level[i][0] < hp_level_downtrend_threshold else 0]
        except BaseException as e:
            rospy.logerr("Error: " + str(e))
            return

        # ==============================

        # ! FOR TESTING ONLY
        # isclose_cso_lcv_lp = [1, 1, 1]
        # isclose_cso_lcv_hp = [1, 1, 1]
        # isclose_damper = [1, 1, 1]
        # ! FOR TESTING ONLY

        # ==============================

        isleak_lp = []
        isleak_hp = []

        try:
            for i in range(3):
                if not isclose_damper[i]:
                    isleak_lp += [0]
                    isleak_hp += [0]
                    continue
                isleak_lp += [1 if trend_lp_level[i] != 0 and isclose_cso_lcv_lp[i] == 1 else 0]
                isleak_hp += [1 if trend_hp_level[i] != 0 and isclose_cso_lcv_hp[i] == 1 else 0]
        except BaseException as e:
            rospy.logerr("Error: " + str(e))
            return

        # ==============================

        columns = ["m11_lp", "m11_hp",
                   "m12_lp", "m12_hp",
                   "m13_lp", "m13_hp",
                   "leak11_lp", "leak11_hp",
                   "leak12_lp", "leak12_hp",
                   "leak13_lp", "leak13_hp"]
        values = [str(m_c_lp_level[0][0]), str(m_c_hp_level[0][0]),
                  str(m_c_lp_level[1][0]), str(m_c_hp_level[1][0]),
                  str(m_c_lp_level[2][0]), str(m_c_hp_level[2][0]),
                  str(isleak_lp[0]), str(isleak_hp[0]),
                  str(isleak_lp[1]), str(isleak_hp[1]),
                  str(isleak_lp[2]), str(isleak_hp[2])]

        try:
            self.cli_db_delete("tbl_eval_kebocoran_feed_water_last", "")
            self.cli_db_insert("tbl_eval_kebocoran_feed_water_last", columns, values)
            self.cli_db_insert("tbl_eval_kebocoran_feed_water", columns, values)
            self.cli_db_delete("tbl_eval_kebocoran_feed_water_last1800sec", "timestamp_local < now() - interval '1800 second'")
            self.cli_db_insert("tbl_eval_kebocoran_feed_water_last1800sec", columns, values)
        except BaseException as e:
            rospy.logerr("Error: " + str(e))
            return


if __name__ == "__main__":
    rospy.init_node("evaluation", anonymous=False)
    evaluation = Evaluation()
    rospy.spin()
