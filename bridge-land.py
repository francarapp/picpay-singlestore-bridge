import pyspark

from pyspark.sql import SparkSession

import core
from core import session
from core.transform import Shape

file = "/Users/frankcara/dev/picpay/singlestore/db/screen/part-00000-00b11c63-af57-4730-9441-aa5074e044fa.c000.snappy.parquet"

df_screen = session.spark.read.parquet(file)
df_screen.printSchema()

Shape(df_screen).show()

session.spark.stop()