import pyspark

from pyspark.sql import SparkSession

import logging
log = logging.getLogger('core.session')

spark = None
context = None

def Session(appName, numExecutors):
    log.info(f"Creating SparkSession for {appName} with {numExecutors} executors")
    global spark, context
    spark = SparkSession\
        .builder\
        .appName(appName)\
        .set("spark.executor.instances", numExecutors)\
        .getOrCreate()
    spark.conf.set("spark.datasource.singlestore.clientEndpoint", "10.164.47.110:3306")
    spark.conf.set("spark.datasource.singlestore.user", "root")
    spark.conf.set("spark.datasource.singlestore.password", "singlestore")
    spark.conf.set("spark.datasource.singlestore.database", "events")
    spark.conf.set("spark.datasource.singlestore.overwriteBehavior", "merge")
    spark.conf.set("spark.datasource.singlestore.maxErrors", 10)

    context = spark.sparkContext
    context.setLogLevel("ERROR")
    return spark    

def init():
    print("INITIALIZING SPARK SESSION")
    global spark, context
    spark = SparkSession\
        .builder\
        .appName("ET2SSBridge")\
        .getOrCreate()
    spark.conf.set("spark.datasource.singlestore.clientEndpoint", "10.164.47.110:3306")
    spark.conf.set("spark.datasource.singlestore.user", "root")
    spark.conf.set("spark.datasource.singlestore.password", "singlestore")
    spark.conf.set("spark.datasource.singlestore.database", "events")
    spark.conf.set("spark.datasource.singlestore.overwriteBehavior", "merge")
    spark.conf.set("spark.datasource.singlestore.maxErrors", 10)

    context = spark.sparkContext
    context.setLogLevel("ERROR")