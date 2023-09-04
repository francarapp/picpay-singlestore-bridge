import pyspark

from pyspark.sql import SparkSession

spark = None
context = None

def init():
    print("INITIALIZING SPARK SESSION")
    global spark, context
    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()
    spark.conf.set("spark.datasource.singlestore.clientEndpoint", "10.164.47.110:3306")
    spark.conf.set("spark.datasource.singlestore.user", "root")
    spark.conf.set("spark.datasource.singlestore.password", "singlestore")
    spark.conf.set("spark.datasource.singlestore.database", "events")
    spark.conf.set("spark.datasource.singlestore.overwriteBehavior", "merge")
    spark.conf.set("spark.datasource.singlestore.maxErrors", 10)

    context = spark.sparkContext    
    context.setLogLevel("ERROR")