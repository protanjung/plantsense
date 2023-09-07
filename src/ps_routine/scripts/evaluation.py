#!/usr/bin/python3

import rospy
from ps_interface.srv import db_insert, db_insertResponse
from ps_interface.srv import db_select, db_selectResponse
from ps_interface.srv import db_update, db_updateResponse
from ps_interface.srv import db_upsert, db_upsertResponse
from ps_interface.srv import db_delete, db_deleteResponse
import sys
import time
import pandas as pd
import numpy as np
import pyromat as pm


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

        self.H2O = pm.get('mp.H2O')

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
        self.steam_turbine_heat_rate()

    # --------------------------------------------------------------------------

    def evaluation_init(self):
        time.sleep(5.0)

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
            # Resample data to get data at every period
            temp = temp.resample(str(period) + "S").mean()
            # Interpolate data to fill NaN value
            temp = temp.interpolate(method="linear", limit_direction="both")
            # Return data
            ret_df = temp.copy()
        except Exception as e:
            rospy.logerr("Error: " + str(e))
            return pd.DataFrame()

        return ret_df

    def get_raw_data_last(self, name):
        '''
        Get raw data from database
        :param name: list of OPC tags
        :return: pandas dataframe with OPC tags as index, and OPC values as values
        '''
        # Construct where clause
        where = "name in ("
        for i in range(len(name)):
            where += "'" + name[i] + "'"
            if i < len(name) - 1:
                where += ", "
        where += ")"

        # Get data from database
        ret_json = self.cli_db_select("tbl_data_last", ["*"], where)
        ret_df = pd.read_json(ret_json.response, orient="split")

        try:
            # Copy data
            temp = ret_df.copy()
            # Pivot data to get timestamp as index, OPC tags as columns, and OPC values as values
            temp = temp.pivot(index="timestamp", columns="name", values="value")
            # Calculate average of each column to remove NaN value
            temp = temp.mean(axis=0).to_frame("value")
            # Return data
            ret_df = temp.copy()
        except Exception as e:
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
                raw_damper_full_open = self.get_raw_data_last(tag_damper_full_open[i]).iloc[0, 0]
                raw_damper_full_close = self.get_raw_data_last(tag_damper_full_close[i]).iloc[0, 0]
                isopen_damper += [1 if raw_damper_full_open > 0.5 else 0]
                isclose_damper += [1 if raw_damper_full_close > 0.5 else 0]
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            rospy.logerr("Error: " + str(e) + " at " + str(exc_tb.tb_lineno))
            return

        # ==============================

        tag_cso_lcv_lp = [["P.B11GTHRSG.H1AO00004"],
                          ["P.B12GTHRSG.H2AO00004"],
                          ["P.B13GTHRSG.H3AO00004"]]
        tag_cso_lcv_hp = [["P.B11GTHRSG.H1AO00005"],
                          ["P.B12GTHRSG.H2AO00005"],
                          ["P.B13GTHRSG.H3AO00005"]]
        tag_cso_lcv_hp_sub = [["P.B11GTHRSG.H1LA00400"],
                              ["P.B12GTHRSG.H2LA00400"],
                              ["P.B13GTHRSG.H3LA00400"]]

        isclose_cso_lcv_lp = []
        isclose_cso_lcv_hp = []

        try:
            for i in range(3):
                raw_cso_lcv_lp = self.get_raw_data_last(tag_cso_lcv_lp[i]).iloc[0, 0]
                raw_cso_lcv_hp = self.get_raw_data_last(tag_cso_lcv_hp[i]).iloc[0, 0]
                raw_cso_lcv_hp_sub = self.get_raw_data_last(tag_cso_lcv_hp_sub[i]).iloc[0, 0]
                isclose_cso_lcv_lp += [1 if raw_cso_lcv_lp < 0.01 else 0]
                isclose_cso_lcv_hp += [1 if (raw_cso_lcv_hp < 0.01 and raw_cso_lcv_hp_sub < 0.01) else 0]
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            rospy.logerr("Error: " + str(e) + " at " + str(exc_tb.tb_lineno))
            return

        # ==============================

        lp_level_uptrend_threshold = [float(rospy.get_param("lp_level_uptrend_threshold_11", 0.05)),
                                      float(rospy.get_param("lp_level_uptrend_threshold_12", 0.05)),
                                      float(rospy.get_param("lp_level_uptrend_threshold_13", 0.05))]
        lp_level_downtrend_threshold = [float(rospy.get_param("lp_level_downtrend_threshold_11", -1000)),
                                        float(rospy.get_param("lp_level_downtrend_threshold_12", -1000)),
                                        float(rospy.get_param("lp_level_downtrend_threshold_13", -1000))]
        hp_level_uptrend_threshold = [float(rospy.get_param("hp_level_uptrend_threshold_11", 0.05)),
                                      float(rospy.get_param("hp_level_uptrend_threshold_12", 0.05)),
                                      float(rospy.get_param("hp_level_uptrend_threshold_13", 0.05))]
        hp_level_downtrend_threshold = [float(rospy.get_param("hp_level_downtrend_threshold_11", -1000)),
                                        float(rospy.get_param("hp_level_downtrend_threshold_12", -1000)),
                                        float(rospy.get_param("hp_level_downtrend_threshold_13", -1000))]

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
                trend_lp_level += [1 if m_c_lp_level[i][0] > lp_level_uptrend_threshold[i] else -1 if m_c_lp_level[i][0] < lp_level_downtrend_threshold[i] else 0]
                trend_hp_level += [1 if m_c_hp_level[i][0] > hp_level_uptrend_threshold[i] else -1 if m_c_hp_level[i][0] < hp_level_downtrend_threshold[i] else 0]
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            rospy.logerr("Error: " + str(e) + " at " + str(exc_tb.tb_lineno))
            return

        # ==============================

        isleak_lp = []
        isleak_hp = []

        for i in range(3):
            # If damper is not fully closed,
            # then there is no leak
            if isclose_damper[i] != 1:
                isleak_lp += [0]
                isleak_hp += [0]
                continue
            # If damper is fully closed,
            # then there is leak if trend is not 0 and cso lcv is closed
            isleak_lp += [1 if trend_lp_level[i] != 0 and isclose_cso_lcv_lp[i] == 1 else 0]
            isleak_hp += [1 if trend_hp_level[i] != 0 and isclose_cso_lcv_hp[i] == 1 else 0]
            continue

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
            self.cli_db_insert("tbl_eval_kebocoran_feed_water", columns, values)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            rospy.logerr("Error: " + str(e) + " at " + str(exc_tb.tb_lineno))
            return

    def steam_turbine_heat_rate(self):
        tag_lp_temperature = [["P.B1PLANT.STAI00014"],
                              ["P.B2PLANT.STAI00014"],
                              ["P.B3PLANT.STAI00014"]]
        tag_lp_pressure = [["P.B1PLANT.STLA00133"],
                           ["P.B2PLANT.STLA00133"],
                           ["P.B3PLANT.STLA00133"]]
        tag_hp_temperature = [["P.B1PLANT.CCAI00002"],
                              ["P.B2PLANT.CCAI00002"],
                              ["P.B3PLANT.CCAI00002"]]
        tag_hp_pressure = [["P.B1PLANT.STLA00132"],
                           ["P.B2PLANT.STLA00132"],
                           ["P.B3PLANT.STLA00132"]]

        lp_pressure = []
        lp_temperature = []
        hp_pressure = []
        hp_temperature = []

        try:
            for i in range(3):
                raw_lp_pressure = self.get_raw_data_last(tag_lp_pressure[i]).iloc[0, 0]
                raw_lp_temperature = self.get_raw_data_last(tag_lp_temperature[i]).iloc[0, 0]
                raw_hp_pressure = self.get_raw_data_last(tag_hp_pressure[i]).iloc[0, 0]
                raw_hp_temperature = self.get_raw_data_last(tag_hp_temperature[i]).iloc[0, 0]
                lp_pressure += [raw_lp_pressure]
                lp_temperature += [raw_lp_temperature]
                hp_pressure += [raw_hp_pressure]
                hp_temperature += [raw_hp_temperature]
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            rospy.logerr("Error: " + str(e) + " at " + str(exc_tb.tb_lineno))
            return

        # ==============================

        tag_gland_temperature = [["P.B1PLANT.DALA00345"],
                                 ["P.B2PLANT.DALA00345"],
                                 ["P.B3PLANT.DAAI00345"]]
        tag_gland_pressure = [["P.B1PLANT.BPAI00005"],
                              ["P.B2PLANT.BPAI00005"],
                              ["P.B3PLANT.BPAI00005"]]

        gland_temperature = []
        gland_pressure = []

        try:
            for i in range(3):
                raw_gland_temperature = self.get_raw_data_last(tag_gland_temperature[i]).iloc[0]
                raw_gland_pressure = self.get_raw_data_last(tag_gland_pressure[i]).iloc[0]
                gland_temperature += [raw_gland_temperature]
                gland_pressure += [raw_gland_pressure]
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            rospy.logerr("Error: " + str(e) + " at " + str(exc_tb.tb_lineno))
            return

        # ==============================

        tag_lp_steam_flow = [["P.B11GTHRSG.H1LA00212", "P.B12GTHRSG.H2LA00212", "P.B13GTHRSG.H3LA00212"],
                             ["P.B21GTHRSG.H1LA00212", "P.B22GTHRSG.H2LA00212", "P.B23GTHRSG.H3LA00212"],
                             ["P.B31GTHRSG.H1LA00212", "P.B32GTHRSG.H2LA00212", "P.B33GTHRSG.H3LA00212"]]
        tag_hp_steam_flow = [["P.B11GTHRSG.H1LA00214", "P.B12GTHRSG.H2LA00214", "P.B13GTHRSG.H3LA00214"],
                             ["P.B21GTHRSG.H1LA00214", "P.B22GTHRSG.H2LA00214", "P.B23GTHRSG.H3LA00214"],
                             ["P.B31GTHRSG.H1LA00214", "P.B32GTHRSG.H2LA00214", "P.B33GTHRSG.H3LA00214"]]

        lp_steam_flow = []
        hp_steam_flow = []

        try:
            for i in range(3):
                raw_lp_steam_flow = self.get_raw_data_last(tag_lp_steam_flow[i])
                raw_hp_steam_flow = self.get_raw_data_last(tag_hp_steam_flow[i])
                sum_lp_steam_flow = raw_lp_steam_flow.sum(axis=0).iloc[0]
                sum_hp_steam_flow = raw_hp_steam_flow.sum(axis=0).iloc[0]
                lp_steam_flow += [sum_lp_steam_flow]
                hp_steam_flow += [sum_hp_steam_flow]
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            rospy.logerr("Error: " + str(e) + " at " + str(exc_tb.tb_lineno))
            return

        # ==============================

        tag_mw = [["P.B1PLANT.STAI00010"],
                  ["P.B2PLANT.STAI00010"],
                  ["P.B3PLANT.STAI00010"]]

        mw = []

        try:
            for i in range(3):
                raw_mw = self.get_raw_data_last(tag_mw[i]).iloc[0, 0]
                mw += [raw_mw]
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            rospy.logerr("Error: " + str(e) + " at " + str(exc_tb.tb_lineno))
            return

        # ==============================

        lp_enthalpy = [self.get_enthalpy(lp_temperature[i], lp_pressure[i]) for i in range(3)]
        hp_enthalpy = [self.get_enthalpy(hp_temperature[i], hp_pressure[i]) for i in range(3)]
        gland_enthalpy = [self.get_enthalpy(gland_temperature[i], gland_pressure[i]) for i in range(3)]

        st_heat_rate_blok123 = [self.get_heat_rate(lp_enthalpy[i], hp_enthalpy[i], gland_enthalpy[i], lp_steam_flow[i], hp_steam_flow[i], mw[i]) for i in range(3)]
        st_heat_rate_total = sum(st_heat_rate_blok123)

        # ==============================

        columns = ["blok1",
                   "blok2",
                   "blok3",
                   "total"]
        values = [str(st_heat_rate_blok123[0]),
                  str(st_heat_rate_blok123[1]),
                  str(st_heat_rate_blok123[2]),
                  str(st_heat_rate_total)]

        try:
            self.cli_db_insert("tbl_eval_st_heat_rate", columns, values)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            rospy.logerr("Error: " + str(e) + " at " + str(exc_tb.tb_lineno))
            return

    # --------------------------------------------------------------------------

    def get_enthalpy(self, temperature, pressure):
        '''
        Calculates the enthalpy of water vapor at a given temperature and pressure.
        :param temperature: Temperature in Celcius
        :param pressure: Pressure in kg/cm2
        :return: Enthalpy in kJ/kg
        '''
        temperature_ = temperature + 273
        pressure_ = pressure * 0.980665

        return self.H2O.h(T=temperature_, p=pressure_)[0]

    def get_heat_rate(self, lp_steam_enthalpy, hp_steam_enthalpy, gland_steam_enthalpy, lp_steam_flow, hp_steam_flow, mw):
        '''
        Calculates the heat rate of a turbine.
        :param lp_steam_enthalpy: Enthalpy of low pressure steam in kJ/kg
        :param hp_steam_enthalpy: Enthalpy of high pressure steam in kJ/kg
        :param gland_steam_enthalpy: Enthalpy of gland steam in kJ/kg
        :param lp_steam_flow: Flow of low pressure steam in t/h
        :param hp_steam_flow: Flow of high pressure steam in t/h
        :param mw: Power output in MW
        :return: Heat rate in kcal/kWh
        '''
        A = hp_steam_flow * 1000 * (hp_steam_enthalpy - gland_steam_enthalpy)
        B = lp_steam_flow * 1000 * (lp_steam_enthalpy - gland_steam_enthalpy)
        C = mw

        return (A + B) / C / 4186.8


if __name__ == "__main__":
    rospy.init_node("evaluation", anonymous=False)
    evaluation = Evaluation()
    rospy.spin()
