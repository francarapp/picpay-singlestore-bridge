import sys

from pyspark.sql import SparkSession

from core.extract import Stream, SinkToSS, SinkToConsole
from core.transform import  Shape
from core.filter import Filter
from conf import partitionby, partitionEvName

import logging
log = logging.getLogger(__name__)

def Bridge(landing, table, partitions=[], console=False):
    partitionedby = ""
    evname = landing
    if len(partitions) > 0:
        partitionedby = partitionby(landing, partitions)
        evname = partitionEvName(landing, partitions)
        
    source = f's3a://picpay-datalake-stream-landing/sparkstreaming/et/raw/{landing}-events-approved/'
    
    log.info(f"Bridging from {source} to {table}")
    stream = Filter(
        Shape(
            Stream(source, partition = partitionedby), name=evname
        )
    )
    
    if console:
        log.info("Streamming to console")
        return SinkToConsole(stream)
    
    log.info("Streamming to SS")
    return SinkToSS(stream, table)
