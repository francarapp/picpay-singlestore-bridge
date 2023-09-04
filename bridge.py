import sys

from pyspark.sql import SparkSession

from core.extract import Stream, SinkToSS
from core.transform import  Shape
from core.filter import filterYear

import logging
log = logging.getLogger(__name__)

def Bridge(landing, singlestore):
    source = f's3a://picpay-datalake-stream-landing/sparkstreaming/et/raw/{landing}-events-approved/'
    log.info(f"Bridging from {source} to {singlestore}")
    stream = filterYear(Shape(
        Stream(source)
    ), "2023")
    
    return SinkToSS(stream, singlestore)
