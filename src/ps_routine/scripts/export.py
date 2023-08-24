#!/usr/bin/python3

import rospy
from ps_interface.msg import opc, opcs
from ps_interface.srv import db_insert, db_insertResponse
from ps_interface.srv import db_select, db_selectResponse
from ps_interface.srv import db_update, db_updateResponse
from ps_interface.srv import db_upsert, db_upsertResponse
from ps_interface.srv import db_delete, db_deleteResponse
import sys
import time
import threading
import pandas as pd
import numpy as np
from flask import Flask, request, jsonify
from flask_cors import CORS


class Export():
    def __init__(self):
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

        return 0

    # --------------------------------------------------------------------------

    app = Flask(__name__)
    CORS(app)

    @app.route("/export", methods=["POST"])
    def flask_export():
        param = {}
        param["tags"] = request.form.getlist("tags")
        try:
            param["time_start"] = request.form["time_start"]
        except BaseException:
            pass
        try:
            param["time_start"] = request.form["time_begin"]
        except BaseException:
            pass
        try:
            param["time_stop"] = request.form["time_stop"]
        except BaseException:
            pass
        try:
            param["time_stop"] = request.form["time_end"]
        except BaseException:
            pass

        return jsonify(param)

    def thread_flask(self):
        self.app.run(host="0.0.0.0", port=5001)


if __name__ == "__main__":
    rospy.init_node("export", anonymous=False)
    export = Export()
    rospy.spin()
