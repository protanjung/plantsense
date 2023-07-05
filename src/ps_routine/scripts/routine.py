#!/usr/bin/python

import rospy
from ps_interface.msg import opc, opcs
from ps_interface.srv import db_insert, db_insertResponse
from ps_interface.srv import db_select, db_selectResponse
from ps_interface.srv import db_update, db_updateResponse
from ps_interface.srv import db_upsert, db_upsertResponse
from ps_interface.srv import db_delete, db_deleteResponse
import time
import pandas as pd


class Routine():
    def __init__(self):
        # =====Timer
        self.tim_2hz = rospy.Timer(rospy.Duration(0.5), self.cllbck_tim_2hz)
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

        if self.routine_init() == -1:
            rospy.signal_shutdown("")

    # --------------------------------------------------------------------------

    def cllbck_tim_2hz(self, event):
        pass

    def cllbck_tim_1hz(self, event):
        pass

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
        return 0


if __name__ == "__main__":
    rospy.init_node("routine", anonymous=False)
    routine = Routine()
    rospy.spin()
