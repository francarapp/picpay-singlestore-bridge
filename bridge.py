import sys

from pyspark.sql import SparkSession

from core.extract import Stream, SinkToSS, SinkToConsole
from core.transform import  Shape
from core.filter import Filter
from conf import partitionby

import logging
log = logging.getLogger(__name__)

def Bridge(landing, table, partitions=[], console=False):
    partitionedby = ""
    if len(partitions) > 0:
        partitionedby = partitionby(landing, partitions)
        
    source = f's3a://picpay-datalake-stream-landing/sparkstreaming/et/raw/{landing}-events-approved/{partitionedby}'
    
    log.info(f"Bridging from {source} to {table}")
    stream = Filter(
        Shape(
            Stream(source), name=landing
        )
    )
    
    if console:
        log.info("Streamming to console")
        return SinkToConsole(stream)
    
    log.info("Streamming to SS")
    return SinkToSS(stream, table)
