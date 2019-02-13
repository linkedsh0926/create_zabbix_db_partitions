# create_zabbix_db_partitions
本项目为 Zabbix MySQL 历史表和趋势表提供自动化分区处理。

This project provides automated partitioning for Zabbix MySQL history and trend tables.

## 1. 原理 / Theory
脚本在每次执行时，会先检查并清理过期数据，然后创建新的分区。脚本默认会提前创建7天的历史数据表分区和3个月的趋势数据表分区。这也便于你提前发现问题。

Each time the script executes, it checks and cleans up the stale data and then creates a new partition. By default, the script creates a 7-day historical data table partition and a 3-month trend data table partition in advance. This also makes it easy for you to find problems in advance.

当你初次运行本脚本时，脚本会将所有的旧数据（如果有）统一放在当前日期对应的分区中，这样做是为了加快处理速度，避免锁表现象出现。

When you run this script for the first time, the script will put all the old data (if any) in the partition corresponding to the current date. This is to speed up the processing and avoid the lock table phenomenon.

当你的历史数据很庞大时，建立分区表之后性能提升并不明显，这是因为因为采用了上述策略，即：所有的历史数据都被放在了一个分区中。

When your historical data is very large, the performance improvement after building the partition table is not obvious, because the above strategy is adopted, that is, all historical data is placed in a partition.

**所以，初次执行请尽量先清空旧数据！**

**So, for the first time, please try to empty the old data first!**

## 2. 配置 / Configuration
在运行脚本之前，你需要修改配置。

You need to modify the configuration before running the script.
```
# Zabbix MySQL 数据库连接信息
ZABBIX_DB_SERVER = "127.0.0.1"    # Zabbix MySQL 数据库地址
ZABBIX_DB_USER = "root"    # Zabbix MySQL 数据库账户
ZABBIX_DB_PASS = "password"    # Zabbix MySQL 数据库密码
# Zabbix 历史数据 & 趋势数据保留信息
HISTORY_RETENTION_DAYS = 30    # 历史数据保留时间(天)
HISTORY_PARTITION_IN_ADVANCE_DAYS = 7    # 历史数据表分区提前创建时间(天)
TRENDS_RETENTION_MONTHS = 12    # 趋势数据保留时间(月)
TRENDS_PARTITION_IN_ADVANCE_MONTHS = 3    # 趋势数据表分区提前创建时间(月)
# Zabbix 历史数据 & 趋势数据(表)名
DATABASE_NAME = "zabbix"    # Zabbix 数据库名称
HISTORY_TABLES = ["history", "history_log",
"history_str", "history_text", "history_uint"]
TRENDS_TABLES = ["trends", "trends_uint"]
```
## 3. 运行 / Execute
```
[shell]# python CreateZabbixDBPartitions.py
2019-02-13 13:05:33,936 - INFO: Currently working on table history.
2019-02-13 13:06:14,387 - WARNING: Table history is a table without partitions. Creating initial partition p20190213.
2019-02-13 13:06:14,572 - WARNING: Partition p20190220 does not exist and has been created.
2019-02-13 13:06:14,603 - WARNING: Partition p20190219 does not exist and has been created.
2019-02-13 13:06:14,627 - WARNING: Partition p20190218 does not exist and has been created.
2019-02-13 13:06:14,651 - WARNING: Partition p20190217 does not exist and has been created.
2019-02-13 13:06:14,675 - WARNING: Partition p20190216 does not exist and has been created.
2019-02-13 13:06:14,698 - WARNING: Partition p20190215 does not exist and has been created.
2019-02-13 13:06:14,723 - WARNING: Partition p20190214 does not exist and has been created.
......
2019-02-13 13:06:14,734 - INFO: Currently working on table trends.
2019-02-13 13:06:14,735 - WARNING: Table trends is a table without partitions. Creating initial partition p201902.
2019-02-13 13:06:14,735 - WARNING: Partition p201905 does not exist and has been created.
2019-02-13 13:06:14,735 - WARNING: Partition p201904 does not exist and has been created.
2019-02-13 13:06:14,735 - WARNING: Partition p201903 does not exist and has been created.
......
```
## 4. 检查 / Check
```
MariaDB [(none)]> use zabbix;

Database changed
MariaDB [zabbix]> show create table history_str\G;
*************************** 1. row ***************************
       Table: history_str
Create Table: CREATE TABLE `history_str` (
  `itemid` bigint(20) unsigned NOT NULL,
  `clock` int(11) NOT NULL DEFAULT 0,
  `value` varchar(255) COLLATE utf8_bin NOT NULL DEFAULT '',
  `ns` int(11) NOT NULL DEFAULT 0,
  KEY `history_str_1` (`itemid`,`clock`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
 PARTITION BY RANGE (`clock`)
(PARTITION `p20190213` VALUES LESS THAN (1550073600) ENGINE = InnoDB,
 PARTITION `p20190214` VALUES LESS THAN (1550160000) ENGINE = InnoDB,
 PARTITION `p20190215` VALUES LESS THAN (1550246400) ENGINE = InnoDB,
 PARTITION `p20190216` VALUES LESS THAN (1550332800) ENGINE = InnoDB,
 PARTITION `p20190217` VALUES LESS THAN (1550419200) ENGINE = InnoDB,
 PARTITION `p20190218` VALUES LESS THAN (1550505600) ENGINE = InnoDB,
 PARTITION `p20190219` VALUES LESS THAN (1550592000) ENGINE = InnoDB,
 PARTITION `p20190220` VALUES LESS THAN (1550678400) ENGINE = InnoDB)
1 row in set (0.000 sec)

ERROR: No query specified
MariaDB [zabbix]>
```
如果你看到分区被正确创建了，那么一切正常。

If you see that the partition was created correctly, then everything works fine.
## 5. 设置定时执行 / Set cron job
你需要每天执行一次脚本，清理过期数据并创建新的分区。

You need to execute the script once a day, clean up the outdated data and create a new partition.
```
[shell]# crontab -e
1 0 * * * python /opt/zabbix4/share/zabbix/externalscripts/create_zabbix_db_partitions/CreateZabbixDBPartitions.py
```
## 6. 关闭 Zabbix Housekeeper / Shutdown Zabbix Housekeeper
当开启了表分区后，脚本会自动清理过期数据，所以自带的 Housekeeper 就毫无必要了，而且会加重 MySQL 数据库的负担。我们需要关掉它。

When the table partition is turned on, the script will automatically clean up the expired data, so the housekeeper that comes with it is unnecessary, and will burden the MySQL database. We need to turn it off.

导航至：[管理] - [一般] - [管家]，取消“历史记录”和“趋势”部分下 <开启内部管家> 复选框的勾选。

Navigate to: [Administration] - [General] - [Housekeeping], uncheck the <Enable internal housekeeping> check box under the "History" and "Trends" sections.

