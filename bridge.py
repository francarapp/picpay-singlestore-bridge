import sys

from pyspark.sql import SparkSession

from core import session
from core.transform import Shape

args = sys.argv[1:]
if len(sys.argv) != 2:
   print("Usage: bridge <s3|local directory>", file=sys.stderr)
   sys.exit(-1)
   
#file = "/Users/frankcara/dev/picpay/singlestore/db/screen/part-00000-00b11c63-af57-4730-9441-aa5074e044fa.c000.snappy.parquet"
file = args[0]

df = session.spark.read.parquet(file)
df.printSchema()

df = Shape(df)
df.show()

session.spark.stop()