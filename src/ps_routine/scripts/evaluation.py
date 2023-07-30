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
        self.kebocoran_feed_water()

    # --------------------------------------------------------------------------

    def evaluation_init(self):
        time.sleep(2.0)

        return 0

    # --------------------------------------------------------------------------

    def get_raw_data(self, name, window, period):
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
        print("+============================+")
        print("|    kebocoran_feed_water    |")
        print("+============================+")

        # ==============================

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
                raw_damper_full_open = self.get_raw_data(tag_damper_full_open[i], 10, 10)
                raw_damper_full_close = self.get_raw_data(tag_damper_full_close[i], 10, 10)
                # print("raw_damper_full_open" + str(i))
                # print(raw_damper_full_open.to_markdown())
                # print("raw_damper_full_close" + str(i))
                # print(raw_damper_full_close.to_markdown())
                average_damper_full_open = self.get_average_per_column(raw_damper_full_open).iloc[0, 0]
                average_damper_full_close = self.get_average_per_column(raw_damper_full_close).iloc[0, 0]
                # print("average_damper_full_open" + str(i))
                # print(average_damper_full_open)
                # print("average_damper_full_close" + str(i))
                # print(average_damper_full_close)
                isopen_damper += [True if average_damper_full_open > 0.99 else False]
                isclose_damper += [True if average_damper_full_close > 0.99 else False]
        except BaseException as e:
            isopen_isclose_damper_ok = False
            rospy.logerr("Error: " + str(e))
            return

        print("isopen_damper")
        print(isopen_damper)
        print("isclose_damper")
        print(isclose_damper)

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
                if not isclose_damper[i]:
                    isclose_cso_lcv_lp += [False]
                    isclose_cso_lcv_hp += [False]
                    continue

                raw_cso_lcv_lp = self.get_raw_data(tag_cso_lcv_lp[i], 10, 10)
                raw_cso_lcv_hp = self.get_raw_data(tag_cso_lcv_hp[i], 10, 10)
                # print("raw_cso_lcv_lp" + str(i))
                # print(raw_cso_lcv_lp.to_markdown())
                # print("raw_cso_lcv_hp" + str(i))
                # print(raw_cso_lcv_hp.to_markdown())
                average_cso_lcv_lp = self.get_average_per_column(raw_cso_lcv_lp).iloc[0, 0]
                average_cso_lcv_hp = self.get_average_per_column(raw_cso_lcv_hp).iloc[0, 0]
                # print("average_cso_lcv_lp" + str(i))
                # print(average_cso_lcv_lp)
                # print("average_cso_lcv_hp" + str(i))
                # print(average_cso_lcv_hp)

                isclose_cso_lcv_lp += [True if average_cso_lcv_lp > 99.99 else False]
                isclose_cso_lcv_hp += [True if average_cso_lcv_hp > 99.99 else False]
        except BaseException as e:
            rospy.logerr("Error: " + str(e))
            return

        print("isclose_cso_lcv_lp")
        print(isclose_cso_lcv_lp)
        print("isclose_cso_lcv_hp")
        print(isclose_cso_lcv_hp)

        # ==============================

        tag_lp_level = [["P.B11GTHRSG.H1LA00274", "P.B11GTHRSG.H1LA00275", "P.B11GTHRSG.H1LA00276"],
                        ["P.B12GTHRSG.H2LA00274", "P.B12GTHRSG.H2LA00275", "P.B12GTHRSG.H2LA00276"],
                        ["P.B13GTHRSG.H3LA00274", "P.B13GTHRSG.H3LA00275", "P.B13GTHRSG.H3LA00276"]]
        tag_hp_level = [["P.B11GTHRSG.H1LA00277", "P.B11GTHRSG.H1LA00278", "P.B11GTHRSG.H1LA00279"],
                        ["P.B12GTHRSG.H2LA00277", "P.B12GTHRSG.H2LA00278", "P.B12GTHRSG.H2LA00279"],
                        ["P.B13GTHRSG.H3LA00277", "P.B13GTHRSG.H3LA00278", "P.B13GTHRSG.H3LA00279"]]

        trend_lp_level = []
        trend_hp_level = []

        try:
            for i in range(3):
                if not isclose_damper[i]:
                    trend_lp_level += [0]
                    trend_hp_level += [0]
                    continue

                raw_lp_level = self.get_raw_data(tag_lp_level[i], 600, 10)
                raw_hp_level = self.get_raw_data(tag_hp_level[i], 600, 10)
                # print("raw_lp_level" + str(i))
                # print(raw_lp_level.to_markdown())
                # print("raw_hp_level" + str(i))
                # print(raw_hp_level.to_markdown())
                median_lp_level = self.get_median_per_row(raw_lp_level)
                median_hp_level = self.get_median_per_row(raw_hp_level)
                # print("median_lp_level" + str(i))
                # print(median_lp_level.to_markdown())
                # print("median_hp_level" + str(i))
                # print(median_hp_level.to_markdown())
                m_c_lp_level = self.linear_regression(median_lp_level)
                m_c_hp_level = self.linear_regression(median_hp_level)
                # print("m_c_lp_level" + str(i))
                # print(m_c_lp_level)
                # print("m_c_hp_level" + str(i))
                # print(m_c_hp_level)

                trend_lp_level += [1 if m_c_lp_level[0] > 0.1 else -1 if m_c_lp_level[0] < -0.1 else 0]
                trend_hp_level += [1 if m_c_hp_level[0] > 0.1 else -1 if m_c_hp_level[0] < -0.1 else 0]
        except BaseException as e:
            rospy.logerr("Error: " + str(e))
            return

        print("trend_lp_level")
        print(trend_lp_level)
        print("trend_hp_level")
        print(trend_hp_level)

        # ==============================

        isleak_lp = []
        isleak_hp = []

        try:
            for i in range(3):
                if not isclose_damper[i]:
                    isleak_lp += [False]
                    isleak_hp += [False]
                    continue

                isleak_lp += [True if trend_lp_level[i] != 0 else False]
                isleak_hp += [True if trend_hp_level[i] != 0 else False]
        except BaseException as e:
            rospy.logerr("Error: " + str(e))
            return

        print("isleak_lp")
        print(isleak_lp)
        print("isleak_hp")
        print(isleak_hp)


if __name__ == "__main__":
    rospy.init_node("evaluation", anonymous=False)
    evaluation = Evaluation()
    rospy.spin()
