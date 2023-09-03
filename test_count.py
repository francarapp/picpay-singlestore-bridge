import pyspark

from pyspark.sql import SparkSession

from core import session
from core.word import reduceByWord

file = "submit.py"
session.init()

output = reduceByWord(file).collect()
for (wd, count) in output:
    print("%s: %i" % (wd, count))

session.spark.stop()