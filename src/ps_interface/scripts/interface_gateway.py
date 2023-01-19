#!/usr/bin/python3

import rospy
import requests
import time
from ps_common.srv import *

# =====Parameter
gateway_host = None
gateway_port = None
opc_tag_1sec = None
opc_tag_10sec = None
opc_tag_60sec = None
# =====Timer
tim_1hz = None
tim_01hz = None
tim_0017hz = None
# =====ServiceServer
cli_mysql_insert = None

# -----OPC Data and Tag
opc_data_dict = {}
opc_tag_list = []

# --------------------------------------
# ======================================


def cllbck_tim_1hz(event):
    data = opc_read(opc_tag_1sec)
    insert_data(data)


def cllbck_tim_01hz(event):
    data = opc_read(opc_tag_10sec)
    insert_data(data)


def cllbck_tim_0017hz(event):
    data = opc_read(opc_tag_60sec)
    insert_data(data)

# --------------------------------------
# ======================================


def opc_reset():
    global gateway_host
    global gateway_port

    url = 'http://{}:{}/reset'.format(gateway_host, gateway_port)

    response_string = ''
    response_dict = {}
    try:
        response_string = requests.get(url, timeout=1).text
        response_dict = eval(response_string)

        if response_dict['status'] == 0:
            return 0
        elif response_dict['status'] == -1:
            return -1

    except Exception as e:
        rospy.logerr('Error HTTP Request: {}'.format(e))
        return -1


def opc_list():
    global gateway_host
    global gateway_port

    url = 'http://{}:{}/list'.format(gateway_host, gateway_port)

    response_string = ''
    response_dict = {}
    try:
        response_string = requests.get(url, timeout=1).text
        response_dict = eval(response_string)

        if response_dict['status'] == 0:
            return 0
        elif response_dict['status'] == -1:
            return -1

    except Exception as e:
        rospy.logerr('Error HTTP Request: {}'.format(e))
        return -1


def opc_register(opc_tag_list, period):
    global gateway_host
    global gateway_port

    url = 'http://{}:{}/register/{}'.format(gateway_host, gateway_port, period)

    response_string = ''
    response_dict = {}
    try:
        response_string = requests.get(url, timeout=1, data={'tags': opc_tag_list}).text
        response_dict = eval(response_string)

        if response_dict['status'] == 0:
            return 0
        elif response_dict['status'] == -1:
            return -1

    except Exception as e:
        rospy.logerr('Error HTTP Request: {}'.format(e))
        return -1


def opc_read(opc_tag_list):
    global gateway_host
    global gateway_port

    url = 'http://{}:{}/read'.format(gateway_host, gateway_port)

    response_string = ''
    response_dict = {}
    try:
        response_string = requests.get(url, timeout=1, data={'tags': opc_tag_list}).text
        response_dict = eval(response_string)

        if response_dict['status'] == 0:
            return response_dict['data']
        elif response_dict['status'] == -1:
            return {}

    except Exception as e:
        rospy.logerr('Error HTTP Request: {}'.format(e))
        return {}

# --------------------------------------
# ======================================


def insert_data(opc_data_dict):
    global cli_mysql_insert

    for key in opc_data_dict:
        # Creating a request object for the service mysql_insert.
        # The request object is defined in the service file.
        req_mysql_insert = mysql_insertRequest()
        req_mysql_insert.name = opc_data_dict[key]['name']
        req_mysql_insert.value = opc_data_dict[key]['value']
        req_mysql_insert.quality = opc_data_dict[key]['quality']
        req_mysql_insert.timestamp = opc_data_dict[key]['timestamp']

        # Calling the service mysql_insert.
        # If it fails, it returns -1.
        # If it succeeds, it returns 0.
        try:
            cli_mysql_insert(req_mysql_insert)
            return 0
        except Exception as e:
            return -1

# --------------------------------------
# ======================================


if __name__ == '__main__':
    rospy.init_node('interface_gateway')

    # =====Parameter
    gateway_host = rospy.get_param('gateway/host')
    gateway_port = rospy.get_param('gateway/port')
    opc_tag_1sec = rospy.get_param('opc_tag/1sec')
    opc_tag_10sec = rospy.get_param('opc_tag/10sec')
    opc_tag_60sec = rospy.get_param('opc_tag/60sec')
    # =====Timer
    tim_1hz = rospy.Timer(rospy.Duration(1), cllbck_tim_1hz)
    tim_01hz = rospy.Timer(rospy.Duration(10), cllbck_tim_01hz)
    tim_0017hz = rospy.Timer(rospy.Duration(60), cllbck_tim_0017hz)
    # =====ServiceClient
    cli_mysql_insert = rospy.ServiceProxy('mysql/insert', mysql_insert)

    opc_reset()
    opc_list()
    opc_register(opc_tag_1sec, '1sec')
    opc_register(opc_tag_10sec, '10sec')
    opc_register(opc_tag_60sec, '60sec')

    rospy.spin()
