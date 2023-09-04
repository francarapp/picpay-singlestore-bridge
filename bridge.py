import sys

from pyspark.sql import SparkSession

from core.extract import Stream, SinkToSS, SinkToConsole, SinkForeachToSS
from core.transform import  Shape
from core.filter import Filter
from conf import partitionby, partitionEvName

import logging
log = logging.getLogger(__name__)

def Bridge(landing, table, partitions=[], console=False, debug=false):
    partitionedby = ""
    evname = landing
    if len(partitions) > 0:
        partitionedby = partitionby(landing, partitions)
        evname = partitionEvName(landing, partitions)
        log.info(f"Using {landing} partition event {evname}")
        
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
    
    if debug:
        return SinkForeachToSS(stream, table)
    
    log.info("Streamming to SS")
    return SinkToSS(stream, table)
