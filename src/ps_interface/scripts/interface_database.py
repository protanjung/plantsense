#!/usr/bin/python3

import rospy
from ps_ros_lib.help_log import help_log
from ps_interface.msg import opc, opcs, fuel_input, fuels_input
from threading import Lock
import mysql.connector

class interface_database:
    def __init__(self):
        # =====Parameter
        self.db_host = rospy.get_param('db/host', 'localhost')
        self.db_port = rospy.get_param('db/port', 3306)
        self.db_database = rospy.get_param('db/database', 'ps_database')
        self.db_user = rospy.get_param('db/user', 'ps_user')
        self.db_password = rospy.get_param('db/password', 'ps_password')
        # =====Timer
        self.tim_01hz = rospy.Timer(rospy.Duration(10), self.cllbck_tim_01hz)
        self.tim_1hz = rospy.Timer(rospy.Duration(1), self.cllbck_tim_1hz)
        self.tim_50hz = rospy.Timer(rospy.Duration(0.02), self.cllbck_tim_50hz)
        # =====Subscriber
        self.sub_opcs = rospy.Subscriber('opcs', opcs, self.cllbck_sub_opcs, queue_size=1)
        # =====Publisher
        self.pub_fuels_input = rospy.Publisher('fuels_input', fuels_input, queue_size=0)
        # =====Help
        self._log = help_log()

        self.mylock = Lock()
        self.mydb = None
        self.mycursor = None
        self.is_initialized = False

        if (self.database_init() == -1):
            rospy.signal_shutdown("")

    # --------------------------------------------------------------------------
    # ==========================================================================

    def cllbck_tim_01hz(self, event):
        if not self.is_initialized:
            return

        self.mylock.acquire()
        result = self.database_select_fuel()
        self.mylock.release()

        if result == -1:
            rospy.signal_shutdown("")

        if len(result) > 0:
            msg_fuels_input = fuels_input()
            for row in result:
                msg_fuel_input = fuel_input()
                msg_fuel_input.name = row[1]
                msg_fuel_input.min_volume = row[2]
                msg_fuel_input.max_volume = row[3]
                msg_fuel_input.price = row[4]
                msg_fuels_input.fuels_input.append(msg_fuel_input)
            self.pub_fuels_input.publish(msg_fuels_input)

    def cllbck_tim_1hz(self, event):
        if not self.is_initialized:
            return

        self.mylock.acquire()
        result = self.database_select_param()
        self.mylock.release()

        if result == -1:
            rospy.signal_shutdown("")

        if len(result) > 0:
            for row in result:
                rospy.set_param(row[1], row[2])

    def cllbck_tim_50hz(self, event):
        pass

    # --------------------------------------------------------------------------
    # ==========================================================================

    def cllbck_sub_opcs(self, msg):
        if not self.is_initialized:
            return

        for opc in msg.opcs:
            self.mylock.acquire()
            self.database_insert_data(opc.name, opc.value, opc.timestamp)
            self.mylock.release()
        self.mylock.acquire()
        self.database_delete_data()
        self.mylock.release()

    # --------------------------------------------------------------------------
    # ==========================================================================

    def database_init(self):
        # Printing the parameters.
        rospy.sleep(2)
        self._log.info("Database Host: " + self.db_host)
        self._log.info("Database Port: " + str(self.db_port))
        self._log.info("Database Database: " + self.db_database)
        self._log.info("Database User: " + self.db_user)
        self._log.info("Database Password: " + self.db_password)

        # Initializing the database.
        self.database_initialize()

        # Mark initialization
        self.is_initialized = True

        return 0

    # --------------------------------------------------------------------------
    # ==========================================================================

    def database_connect(self):
        try:
            self.mydb = mysql.connector.connect(
                host=self.db_host,
                port=self.db_port,
                user=self.db_user,
                password=self.db_password,
                database=self.db_database
            )
            self.mycursor = self.mydb.cursor()
        except Exception as e:
            self._log.error("Database connect error: " + str(e))
            return -1

        return 0

    def database_close(self):
        try:
            self.mycursor.close()
            self.mydb.close()
        except Exception as e:
            self._log.error("Database close error: " + str(e))
            return -1

        return 0

    def database_initialize(self):
        sqls = [
            "CREATE TABLE IF NOT EXISTS `tbl_param` ( \
                `id` int NOT NULL AUTO_INCREMENT, \
                `name` varchar(255) NOT NULL, \
                `value` varchar(255) NOT NULL, \
                PRIMARY KEY (`id`) \
            ) ;",
            "CREATE TABLE IF NOT EXISTS `tbl_data` ( \
                `id` int NOT NULL AUTO_INCREMENT, \
                `name` varchar(255) NOT NULL, \
                `value` float NOT NULL, \
                `timestamp` datetime NOT NULL, \
                `timestamp_local` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP, \
                PRIMARY KEY (`id`) \
            ) ;",
            "CREATE TABLE IF NOT EXISTS `tbl_data_last60sec` ( \
                `id` int NOT NULL AUTO_INCREMENT, \
                `name` varchar(255) NOT NULL, \
                `value` float NOT NULL, \
                `timestamp` datetime NOT NULL, \
                `timestamp_local` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP, \
                PRIMARY KEY (`id`) \
            ) ;",
            "CREATE TABLE IF NOT EXISTS `tbl_data_last60min` ( \
                `id` int NOT NULL AUTO_INCREMENT, \
                `name` varchar(255) NOT NULL, \
                `value` float NOT NULL, \
                `timestamp` datetime NOT NULL, \
                `timestamp_local` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP, \
                PRIMARY KEY (`id`) \
            ) ;",
            "CREATE TABLE IF NOT EXISTS `tbl_data_last` ( \
                `name` varchar(255) NOT NULL, \
                `value` float NOT NULL, \
                `timestamp` datetime NOT NULL, \
                `timestamp_local` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP, \
                PRIMARY KEY (`name`) \
            ) ;",
            "CREATE TABLE IF NOT EXISTS `tbl_event` ( \
                `id` int NOT NULL AUTO_INCREMENT, \
                `name` varchar(255) NOT NULL, \
                `value` float NOT NULL, \
                `timestamp` datetime NOT NULL, \
                `timestamp_local` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP, \
                PRIMARY KEY (`id`) \
            ) ;",
            "CREATE TABLE IF NOT EXISTS `tbl_meta` ( \
                `id` int NOT NULL AUTO_INCREMENT, \
                `name` varchar(255) NOT NULL, \
                `description` varchar(255) NOT NULL, \
                `unit` varchar(255) NOT NULL, \
                PRIMARY KEY (`id`) \
            ) ;",
            "CREATE TABLE IF NOT EXISTS `tbl_fuel` ( \
                `id` int NOT NULL AUTO_INCREMENT, \
                `name` varchar(255) NOT NULL, \
                `min_volume` float NOT NULL DEFAULT 0, \
                `max_volume` float NOT NULL DEFAULT 0, \
                `price` float NOT NULL DEFAULT 0, \
                PRIMARY KEY (`id`) \
            ) ;",
            "CREATE TABLE IF NOT EXISTS `tbl_fuel_perminute` ( \
                `id` int NOT NULL AUTO_INCREMENT, \
                `volume` varchar(255) NOT NULL, \
                `subtotal` float NOT NULL DEFAULT 0, \
                `total` float NOT NULL DEFAULT 0, \
                `power` float NOT NULL DEFAULT 0, \
                PRIMARY KEY (`id`) \
            ) ;",
            "CREATE TABLE IF NOT EXISTS `tbl_fuel_perday` ( \
                `id` int NOT NULL AUTO_INCREMENT, \
                `volume` varchar(255) NOT NULL, \
                `subtotal` float NOT NULL DEFAULT 0, \
                `total` float NOT NULL DEFAULT 0, \
                `power` float NOT NULL DEFAULT 0, \
                PRIMARY KEY (`id`) \
            ) ;",
            "CREATE OR REPLACE VIEW `view_data` AS \
            SELECT \
                `tbl_data`.`id` AS `id`, \
                `tbl_data`.`name` AS `name`, \
                `tbl_data`.`value` AS `value`, \
                `tbl_data`.`timestamp` AS `timestamp`, \
                `tbl_data`.`timestamp_local` AS `timestamp_local`, \
                `tbl_meta`.`description` AS `description`, \
                `tbl_meta`.`unit` AS `unit` \
            FROM \
                `tbl_data` \
                LEFT JOIN `tbl_meta` ON `tbl_data`.`name` = `tbl_meta`.`name` \
            ;",
            "CREATE OR REPLACE VIEW `view_event` AS \
            SELECT \
                `tbl_event`.`id` AS `id`, \
                `tbl_event`.`name` AS `name`, \
                `tbl_event`.`value` AS `value`, \
                `tbl_event`.`timestamp` AS `timestamp`, \
                `tbl_event`.`timestamp_local` AS `timestamp_local`, \
                `tbl_meta`.`description` AS `description`, \
                `tbl_meta`.`unit` AS `unit` \
            FROM \
                `tbl_event` \
                LEFT JOIN `tbl_meta` ON `tbl_event`.`name` = `tbl_meta`.`name` \
            ;"
        ]

        try:
            if self.database_connect() == -1:
                return -1

            for sql in sqls:
                self.mycursor.execute(sql)
                self.mydb.commit()

            if self.database_close() == -1:
                return -1
        except Exception as e:
            self._log.error("Database initialize error: " + str(e))
            return -1

        return 0

    # --------------------------------------------------------------------------
    # ==========================================================================

    def database_insert_data(self, opc_name, opc_value, opc_timestamp):
        sqls = ["INSERT INTO {}.tbl_data (name,value,`timestamp`) VALUES (%s,%s,%s)".format(self.db_database),
                "INSERT INTO {}.tbl_data_last60sec (name,value,`timestamp`) VALUES (%s,%s,%s)".format(self.db_database),
                "INSERT INTO {}.tbl_data_last60min (name,value,`timestamp`) VALUES (%s,%s,%s)".format(self.db_database),
                "REPLACE INTO {}.tbl_data_last (name,value,`timestamp`) VALUES (%s,%s,%s)".format(self.db_database)]
        params = [(opc_name, opc_value, opc_timestamp),
                  (opc_name, opc_value, opc_timestamp),
                  (opc_name, opc_value, opc_timestamp),
                  (opc_name, opc_value, opc_timestamp)]

        for sql, param in zip(sqls, params):
            try:
                if self.database_connect() == -1:
                    return -1

                self.mycursor.execute(sql, param)
                self.mydb.commit()

                if self.database_close() == -1:
                    return -1
            except Exception as e:
                self._log.error("Database insert data error: " + str(e))
                return -1

        return 0

    def database_delete_data(self):
        sqls = ["DELETE FROM {}.tbl_data_last60sec WHERE `timestamp_local` < DATE_SUB(NOW(), INTERVAL 60 SECOND)".format(self.db_database),
                "DELETE FROM {}.tbl_data_last60min WHERE `timestamp_local` < DATE_SUB(NOW(), INTERVAL 60 MINUTE)".format(self.db_database)]
        params = [(),
                  ()]

        for sql, param in zip(sqls, params):
            try:
                if self.database_connect() == -1:
                    return -1

                self.mycursor.execute(sql, param)
                self.mydb.commit()

                if self.database_close() == -1:
                    return -1
            except Exception as e:
                self._log.error("Database delete data error: " + str(e))
                return -1

        return 0

    def database_select_fuel(self):
        sql = "SELECT * FROM {}.tbl_fuel".format(self.db_database)

        try:
            if self.database_connect() == -1:
                return -1

            self.mycursor.execute(sql)
            result = self.mycursor.fetchall()

            if self.database_close() == -1:
                return -1
        except Exception as e:
            self._log.error("Database select fuel error: " + str(e))
            return -1

        return result

    def database_select_param(self):
        sql = "SELECT * FROM {}.tbl_param".format(self.db_database)

        try:
            if self.database_connect() == -1:
                return -1

            self.mycursor.execute(sql)
            result = self.mycursor.fetchall()

            if self.database_close() == -1:
                return -1
        except Exception as e:
            self._log.error("Database select param error: " + str(e))
            return -1

        return result


if __name__ == '__main__':
    rospy.init_node('interface_database')
    interface_database()
    rospy.spin()
