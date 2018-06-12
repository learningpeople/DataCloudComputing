# /usr/bin/env python
# -*-coding:utf-8-*-
"""
    __author__:sa

"""
from collections import defaultdict
# import pandas as pd
# from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
# from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
import re
import datetime
import subprocess

date = datetime.datetime.today().date()
date = date.strftime('%Y-%m-%d')


sc = SparkContext(conf=SparkConf().setAppName("The first example"))
path = "hdfs://172.16.201.220:8020/777test/test_flume/{date}/qiji".format(date=date)
textFiles = sc.wholeTextFiles(path)
subprocess.call('sudo -u hdfs hadoop fs -rm -r -f {path}'.format(path=path),shell=True)


def get_kvgroup(table, row):
    group = {}
    keys = [col[:col.find('{')] for col in row.split("|")]
    values = [col[col.find('{') + 1: col.rfind('}')] for col in row.split("|")]
    if table == 'role_login' and 'online_time' in keys and 'type' in keys:
        type_num = values[keys.index('type')]
        num = values[keys.index('online_time')]
        try:
            if int(num) > 1209600 and int(type_num) == 2:
                print 'error: role_login在线时间过长 {}\n'.format(row)
                return
        except Exception as e:
            print e
    for field in row.split("|"):
        start, end = field.find('{'), field.rfind('}')
        k, v = field[:start], field[start + 1:end]
        if k.endswith("_date"):
            k = "ds"
        elif k.endswith("_time"):
            if table == "online":
                k = "time"
            elif k != "online_time":
                k = "time"
        elif k.endswith("_timestamp"):
            k = "timestamp"
        group[k] = v
    return group


data = defaultdict(list)
fields = defaultdict(list)


def process(table, group):
    row = []

    for f in group.keys():
        v = group.get(f, "")
        if v != "":
            row.append(v)
            continue
        row.append(v)
    # data[table].append(row)
    # if table not in fields.keys():
    #     fields[table] = group.keys()
    tb_name ='_'.join((group['gameid'],table))
    data[tb_name].append(row)
    if tb_name not in fields.keys():
        fields[tb_name] = group.keys()


Header = re.compile(r"BI_(?P<table>[\w_]+)\|(?P<other>(.+))")
for (p, v) in textFiles.collect():
    vs = v.split(u'\n')
    for row in vs:
        if not row:
            continue
        header = Header.search(row)
        table, other = header.group("table"), header.group("other")
        group = get_kvgroup(table, other)
        process(table, group)


def tear_down():
    for table in data.keys():
        hiveContext = HiveContext(sc)
        df = hiveContext.createDataFrame(data[table], fields[table])
        hiveContext.sql('use test_db')
        try:
            df.registerTempTable("demo")
            hiveContext.sql("insert into {table} partition(ds='{date}') select * from demo".format(table=table,date=date))
            # hiveContext.sql("insert into {table} partition(ds="")  select * from demo".format(table=table))
        except Exception as e:
            df.saveAsTable("{table}".format(table=table))

tear_down()

    # sqlContext = SQLContext(sc)
    # df = sqlContext.createDataFrame(data[table],fields[table])
    # sqlContext.registerDataFrameAsTable(df, "table1")
    # sqlContext.sql("insert into `table2` partition(date='2015-04-02') select * from table1")
    # df.write.mode("append").insertInto("table2")






        # SparkSession 默认只在spark2.0中
        # spark = SparkSession \
        #     .builder \
        #     .appName("testDataFrame") \
        #     .getOrCreate()
        # sentenceData = spark.createDataFrame(data[table], fields[table])
        # sentenceData.select("label").show()

        # 需要安装pandas
        # df_store = pd.DataFrame(data=data[table], columns=fields[table])
        # df_store.to_csv('%s.csv', header=False, columns=fields[table], index=False, mode='a', encoding="utf8")
#         # # print data_frame
# for table in data.keys():
#     hiveContext = HiveContext(sc)
#     df = hiveContext.createDataFrame(data[table])
#     df.insertInto("test")
#     # df.select("openid").show()
#     # df = hiveContext.createDataFrame(data[table], fields[table])
#
#     # df.write.mode("append").insertInto("test")
#     # df.saveAsTable("test")
#     # df.registerTempTable("demo")
#     # hiveContext.sql("insert into test  select * from demo")



