import sys

from pyspark.sql import SparkSession

from core.extract import Stream, Sink
from core.transform import  Shape
from core.filter import filterYear

def Bridge(landing, singlestore, since):
    stream = filterYear(Shape(
        Stream(f's3a://picpay-datalake-stream-landing/sparkstreaming/et/raw/{landing}-events-approved/', since)
    ), "2023")

    stream = stream.writeStream\
        .option("checkpointLocation", f"/home/spark/checkpoint/{landing}")\
        .option("path", f"s3a://picpay-dataeng-singlestore-landing/events/{singlestore}")\
        .partitionBy("ano","mes", "dia", "hora", "minuto")\
        .format("parquet")
    return stream
