import sys

from pyspark.sql import SparkSession

from core.extract import Stream, Sink
from core.transform import  Shape

def Bridge(landing, singlestore, since):
    stream = Shape(
        Stream(f's3a://picpay-datalake-stream-landing/sparkstreaming/et/raw/{landing}-events-approved/', since)
    )

    stream = stream.writeStream\
        .option("checkpointLocation", "/home/spark/checkpoint/{landing}")\
        .option("path", "s3a://picpay-dataeng-singlestore-landing/events/houston")\
        .partitionBy("ano","mes", "dia", "hora", "minuto")\
        .format("parquet")
    stream.start()
