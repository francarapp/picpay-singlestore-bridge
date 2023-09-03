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

    context = spark.sparkContext    
    context.setLogLevel("ERROR")