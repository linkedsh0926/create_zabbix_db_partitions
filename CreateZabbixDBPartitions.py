#!/usr/bin/python
# -*- coding: utf-8 -*-

# Author: linkedsh
# Email: linkedsh#outlook.com
# Mozilla Public License 2.0
# Date: 2019-02-13
# Funtion: Create paritions for zabbix MySQL tables.
# Repo: https://github.com/linkedsh0926/create_zabbix_db_partitions

import datetime
import logging
import random
import time

import pymysql

from dateutil.relativedelta import relativedelta

# Zabbix MySQL 数据库连接信息
ZABBIX_DB_SERVER = "127.0.0.1"      # Zabbix MySQL 数据库地址
ZABBIX_DB_USER = "root"             # Zabbix MySQL 数据库账户
ZABBIX_DB_PASS = "password"         # Zabbix MySQL 数据库密码

# Zabbix 历史数据 & 趋势数据保留信息
HISTORY_RETENTION_DAYS = 30             # 历史数据保留时间(天)
HISTORY_PARTITION_IN_ADVANCE_DAYS = 7   # 历史数据表分区提前创建时间(天)
TRENDS_RETENTION_MONTHS = 12            # 趋势数据保留时间(月)
TRENDS_PARTITION_IN_ADVANCE_MONTHS = 3  # 趋势数据表分区提前创建时间(月)

# Zabbix 历史数据 & 趋势数据(表)名
DATABASE_NAME = "zabbix"
HISTORY_TABLES = ["history", "history_log",
                  "history_str", "history_text", "history_uint"]
TRENDS_TABLES = ["trends", "trends_uint"]


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s: %(message)s')


class MySQLConn(object):
    def __init__(self, addr, user, password, dbName):
        self.db = pymysql.connect(addr, user, password, dbName)
        self.cursor = self.db.cursor()

    def execute_sql(self, sqlSentence):
        operationID = random.randint(0, 9999)
        startTime = time.time()
        logging.debug("[Operation-%s] Executing SQL statement: %s" % (operationID, sqlSentence))
        operationMethod = sqlSentence.split()[0].upper()
        self.cursor.execute(sqlSentence)
        if operationMethod in ("UPDATE", "DELETE"):
            result = self.db.commit()
        else:
            result = self.cursor.fetchall()

        costTime = time.time() - startTime
        logging.debug("[Operation-%s] Took %s seconds." % (operationID, costTime))

        return result


class MySQLTable(object):
    class Field(object):
        def __init__(self, name, fieldType, isNull, keyType, default, extra):
            self.name = name
            self.fieldType = fieldType
            self.isNull = isNull
            self.keyType = keyType
            self.default = default
            self.extra = extra

    class Partition(object):
        def __init__(self, name, method, expression, description, rows):
            self.name = name
            self.method = method
            self.expression = expression
            self.description = description
            self.rows = rows

    def __init__(self, tableName, mySQLCONN):
        self.name = tableName
        self.conn = mySQLCONN
        self.fields = []
        self.partitions = {}
        self.describe_table()

    def describe_table(self):
        fieldDescRawResult = self.conn.execute_sql("DESC %s" % self.name)
        for fieldDesc in fieldDescRawResult:
            fieldName = fieldDesc[0]
            fieldType = fieldDesc[1]
            fieldIsNull = fieldDesc[2]
            fieldKeyType = fieldDesc[3]
            fieldDefault = fieldDesc[4]
            fieldExtra = fieldDesc[5]

            thisField = self.Field(
                fieldName, fieldType, fieldIsNull, fieldKeyType, fieldDefault, fieldExtra)
            self.fields.append(thisField)

        partitionDescRawResult = self.conn.execute_sql(
            "SELECT PARTITION_NAME, PARTITION_METHOD, PARTITION_EXPRESSION, PARTITION_DESCRIPTION, TABLE_ROWS FROM INFORMATION_SCHEMA.partitions WHERE TABLE_SCHEMA = schema() AND TABLE_NAME='%s'" % self.name)
        for partitionDesc in partitionDescRawResult:
            partitionName = partitionDesc[0]
            partitionMethod = partitionDesc[1]
            partitionExpression = partitionDesc[2]
            partitionDescription = partitionDesc[3]
            partitionRows = partitionDesc[4]

            if not partitionName:
                continue
            handlingPartition = self.Partition(
                partitionName, partitionMethod, partitionExpression, partitionDescription, partitionRows)
            self.partitions[handlingPartition.name] = handlingPartition


class TimeProcessor(object):
    def __init__(self, timeStep):
        self.timeStep = timeStep
        if timeStep == "months":
            self.timeStrFmt = "%Y%m"
        elif timeStep == "days":
            self.timeStrFmt = "%Y%m%d"
        else:
            logging.error("Unknown time step type %s." % timeStep)
            exit()

        self.timeStampNow = int(time.mktime(time.strptime(datetime.datetime.strftime(datetime.datetime.now(), self.timeStrFmt), self.timeStrFmt)))
        self.dateTimeNow = datetime.datetime.fromtimestamp(self.timeStampNow)

    class CurrentPartitionTimeInfo(object):
        def __init__(self, superClass, durationDelta):
            self.dateTime = superClass.dateTimeNow + \
                eval("relativedelta(%s=%s)" % (superClass.timeStep, durationDelta+1))                           # 当前分区 datetime 时间
            self.timeStamp = int(self.dateTime.strftime("%s"))                                                  # 当前分区时间戳
            self.dateTimeLessThan = self.dateTime + eval("relativedelta(%s=%s)" % (superClass.timeStep, 1))     # 当前分区 datetime 时间上界
            self.timeStampLessThan = int(time.mktime(self.dateTimeLessThan.timetuple()))                        # 当前分区时间戳上界
            self.timeStr = self.dateTime.strftime(superClass.timeStrFmt)
            self.name = 'p' + self.timeStr

    class NextPartitionTimeInfo(object):
        def __init__(self, superClass, currentPartitionObject, durationDelta):
            self.timeStamp = currentPartitionObject.timeStampLessThan                                           # 下个分区时间戳
            self.dateTime = datetime.datetime.fromtimestamp(self.timeStamp)                                     # 下个分区 datetime 时间
            self.dateTimeLessThan = self.dateTime + eval("relativedelta(%s=%s)" % (superClass.timeStep, 1))     # 下个分区 datetime 时间上界
            self.timeStampLessThan = int(time.mktime(self.dateTimeLessThan.timetuple()))                        # 下个分区时间戳上界
            self.timeStr = self.dateTime.strftime(superClass.timeStrFmt)                                        # 下个分区字符串格式时间 %Y%m
            self.name = 'p' + self.timeStr                                                                      # 下个分区名称


def create_partitions(tableObj, timeStep, durationRetention, durationInAdvance):
    logging.info("Currently working on table %s." % tableObj.name)

    timeProcessor = TimeProcessor(timeStep)
    dateTimeMin = timeProcessor.dateTimeNow - eval("relativedelta(%s=%s)" % (timeStep, durationRetention))
    dateTimeMax = timeProcessor.dateTimeNow + eval("relativedelta(%s=%s)" % (timeStep, durationInAdvance))
    timeStampMin = int(dateTimeMin.strftime("%s"))                                              # 合法分区时间范围下限
    timeStampMax = int(dateTimeMax.strftime("%s"))                                              # 合法分区时间范围上限

    logging.debug("Partitions in table %s are being cleaned beyond the legal time range." % tableObj.name)
    for _, partition in tableObj.partitions.items():
        partitionTimeStr = partition.name.split('p')[1]                                         # 解析出当前分区字符串格式时间
        try:
            partitionTimeStamp = time.mktime(time.strptime(partitionTimeStr, timeProcessor.timeStrFmt))       # 当前分区时间戳
        except ValueError:
            logging.error("Partition %s cannot be recognized as time format %s, please handle it manually" %
                          (partition.name, timeProcessor.timeStrFmt))
            exit()

        if partitionTimeStamp > timeStampMax:
            # 清空大于合法时间范围的分区
            if partition.rows == 0:
                tableObj.conn.execute_sql("ALTER TABLE %s DROP PARTITION %s;" % (tableObj.name, partition.name))
                logging.warn("Partition %s exceeded the maximum partition advance creation time and was dropped." % partition.name)
            else:
                logging.error(
                    "Partition %s exceeded the maximum partition advance creation time, but it is not empty and has been ignored. Please check the details later." % partition.name)
        elif partitionTimeStamp < timeStampMin:
            # 清空小于合法时间范围的分区
            tableObj.conn.execute_sql("ALTER TABLE %s DROP PARTITION %s;" % (tableObj.name, partition.name))
            logging.info("Partition %s has expired and has been dropped." % partition.name)

    if len(tableObj.partitions) == 0:
        # 表分区不存在，创建初始分区
        initialPartition = timeProcessor.CurrentPartitionTimeInfo(timeProcessor, -1)
        sql = "ALTER TABLE %s PARTITION BY RANGE( clock ) (PARTITION %s VALUES LESS THAN (%s));" % (
            tableObj.name, initialPartition.name, initialPartition.timeStampLessThan)
        tableObj.conn.execute_sql(sql)
        logging.warn("Table %s is a table without partitions. Creating initial partition %s." % (tableObj.name, initialPartition.name))

    for durationDelta in reversed(range(durationInAdvance)):
        # 倒序遍历提前创建的分区，忽略历史分区以及当前分区
        currentPartition = timeProcessor.CurrentPartitionTimeInfo(timeProcessor, durationDelta)
        nextPartition = timeProcessor.NextPartitionTimeInfo(timeProcessor, currentPartition, durationDelta)
        logging.debug("The current operation partition is %s." % currentPartition.name)

        if currentPartition.name in tableObj.partitions.keys():
            handlingPartition = tableObj.partitions[currentPartition.name]
            if int(handlingPartition.description) == currentPartition.timeStampLessThan:
                # 时间戳正确则继续
                logging.info("Partition %s is correctly established." % currentPartition.name)
                continue

            if currentPartition.timeStamp == timeStampMax:
                # 是最后一个分区
                if int(handlingPartition.description) < currentPartition.timeStampLessThan:
                    # 当前分区时间戳小于标准时间，调大
                    sql = "ALTER TABLE %s REORGANIZE PARTITION %s INTO ( PARTITION %s VALUES LESS THAN (%s) );" % (
                        tableObj.name, currentPartition.name, currentPartition.name, currentPartition.timeStampLessThan)
                    tableObj.conn.execute_sql(sql)

                    logging.warn("Partition %s's timestamp range(%s) is below the correct limit(%s) and has been adjusted." %
                                 (currentPartition.name, handlingPartition.description, currentPartition.timeStampLessThan))
                elif int(handlingPartition.description) > currentPartition.timeStampLessThan:
                    # 当前分区时间戳大于标准时间，分割，删除
                    sql = "ALTER TABLE %s REORGANIZE PARTITION %s INTO ( PARTITION %s VALUES LESS THAN (%s), PARTITION ptmp VALUES LESS THAN (%s) );" % (
                        tableObj.name, currentPartition.name, currentPartition.name, currentPartition.timeStampLessThan, handlingPartition.description)
                    tableObj.conn.execute_sql(sql)
                    sql = "ALTER TABLE %s DROP PARTITION ptmp;" % tableObj.name
                    tableObj.conn.execute_sql(sql)
                    logging.warn("Partition %s's timestamp range(%s) is higher the correct limit(%s) and has been adjusted." %
                                 (currentPartition.name, handlingPartition.description, currentPartition.timeStampLessThan))
            else:
                # 不是最后一个分区，统一向上合并，再分割
                sql = "ALTER TABLE %s REORGANIZE PARTITION %s,%s INTO ( PARTITION %s VALUES LESS THAN (%s) );" % (
                    tableObj.name, currentPartition.name, nextPartition.name, nextPartition.name, nextPartition.timeStampLessThan)
                tableObj.conn.execute_sql(sql)

                sql = "ALTER TABLE %s REORGANIZE PARTITION %s INTO ( PARTITION %s VALUES LESS THAN (%s), PARTITION %s VALUES LESS THAN (%s) );" % (
                    tableObj.name, nextPartition.name, currentPartition.name, currentPartition.timeStampLessThan, nextPartition.name, nextPartition.timeStampLessThan)
                tableObj.conn.execute_sql(sql)

                logging.warn("Partition %s's timestamp range(%s) is beyond the correct limit(%s) and has been adjusted." %
                             (currentPartition.name, handlingPartition.description, currentPartition.timeStampLessThan))
        else:
            if currentPartition.timeStamp == timeStampMax:
                # 是最后一个分区，直接添加
                sql = "ALTER TABLE %s ADD PARTITION (PARTITION %s VALUES LESS THAN (%s));" % (
                    tableObj.name, currentPartition.name, currentPartition.timeStampLessThan)
                tableObj.conn.execute_sql(sql)
            else:
                # 不是最后一个分区，从下一个分区中分割
                sql = "ALTER TABLE %s REORGANIZE PARTITION %s INTO ( PARTITION %s VALUES LESS THAN (%s), PARTITION %s VALUES LESS THAN (%s) );" % (
                    tableObj.name, nextPartition.name, currentPartition.name, currentPartition.timeStampLessThan, nextPartition.name, nextPartition.timeStampLessThan)
                tableObj.conn.execute_sql(sql)
            logging.warn("Partition %s does not exist and has been created." % currentPartition.name)


def main():
    mysqlConn = MySQLConn(ZABBIX_DB_SERVER, ZABBIX_DB_USER, ZABBIX_DB_PASS, DATABASE_NAME)

    for historyTable in HISTORY_TABLES:
        tableObj = MySQLTable(historyTable, mysqlConn)
        create_partitions(tableObj, "days", HISTORY_RETENTION_DAYS, HISTORY_PARTITION_IN_ADVANCE_DAYS)

    for trendTable in TRENDS_TABLES:
        tableObj = MySQLTable(trendTable, mysqlConn)
        create_partitions(tableObj, "months", TRENDS_RETENTION_MONTHS, TRENDS_PARTITION_IN_ADVANCE_MONTHS)


if __name__ == '__main__':
    main()
