#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from DorisSinkUtil import DorisSinkUtil

"""
@基本功能: 
@program: python
@author: B1ue
@createTime: 2025/6/15 下午11:48
"""
with SparkSession.builder \
        .appName("hive_final_test") \
        .master("local[3]") \
        .config("spark.executor.instances", "1") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.scheduler.maxRegisteredResourcesWaitingTime", "60s") \
        .enableHiveSupport().getOrCreate() as spark:

    # 开启动态分区
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    # 建表语句
    spark.sql("""
       create table if not exists b1uemusic.Dws_Trade_Album_Sales(
           album_id STRING,
           sales BIGINT,
           amount BIGINT,
           dt STRING
       ) partitioned by (dt)
       """)

    # 计算离线指标
    spark.sql("""
       insert into b1uemusic.Dws_Trade_Album_Sales
       select
           album_id,
           count(order_num) sales,
           sum(amount) amount,
           dt
       from b1uemusic.dwd_fact_trade_album
       group by album_id,dt
       """)

    # 写入Doris
    # DorisSinkUtil(spark, "B1ueMusic.Dws_Trade_Album_Sales", "album_id,sales,amount,dt")
