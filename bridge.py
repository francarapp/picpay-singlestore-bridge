import sys

from pyspark.sql import SparkSession

from core.extract import Stream, SinkToSS
from core.transform import  Shape
from core.filter import filterYear

def Bridge(landing, singlestore):
    stream = filterYear(Shape(
        Stream(f's3a://picpay-datalake-stream-landing/sparkstreaming/et/raw/{landing}-events-approved/')
    ), "2023")
    
    return SinkToSS(stream, singlestore)
