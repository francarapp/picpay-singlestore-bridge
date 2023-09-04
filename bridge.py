import sys

from pyspark.sql import SparkSession

from core.extract import Stream, SinkToSS, LogToConsole
from core.transform import  Shape
from core.filter import Filter

import logging
log = logging.getLogger(__name__)

def Bridge(landing, singlestore, console=False):
    source = f's3a://picpay-datalake-stream-landing/sparkstreaming/et/raw/{landing}-events-approved/'
    log.info(f"Bridging from {source} to {singlestore}")
    stream = Filter(
        Shape(
            Stream(source)
        )
    )
    
    if console:
        return LogToConsole(stream)
    
    return SinkToSS(stream, singlestore)
