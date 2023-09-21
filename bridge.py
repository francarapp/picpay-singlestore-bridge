from core.extract import Stream, SinkToSS, SinkToConsole, SinkForeachToSS
from core.transform import  Shape, Clean
from core.filter import Filter
from conf import partitionby, partitionEvName

import logging
log = logging.getLogger(__name__)

def Bridge(landing, table, partitions=[], console=False, debug=False, notclean=[]):
    partitionedby = ""
    evname = landing
    if len(partitions) > 0:
        partitionedby = partitionby(landing, partitions)
        evname = partitionEvName(landing, partitions)
        log.info(f"Using {landing} partition event {evname}")
        
    source = f's3a://picpay-datalake-stream-landing/sparkstreaming/et/raw/{landing}-events-approved/'
    
    log.info(f"Bridging from {source} to {table}")
    stream = Filter(
        Clean(Shape(
            Stream(source, partition = partitionedby), 
            landing, evname
        ), notclean)
    )
    
    if console:
        log.info("Streamming to console")
        return SinkToConsole(stream)
    
    if debug:
        log.info("Streamming to SS using foreach")
        return SinkForeachToSS(stream, table)
    
    log.info("Streamming to SS")
    return SinkToSS(stream, table)

def BridgeInnerEvents(landing, table, events=[], evgroup=None, console=False, debug=False, notclean=[]):
    evgroup = landing if evgroup is None else evgroup
    ts = ','.join([f"'{t}'" for t in events])
    source = f's3a://picpay-datalake-stream-landing/sparkstreaming/et/raw/{landing}-events-approved/'
    
    log.info(f"Bridging from {source} to {table} in group {evgroup}")
    stream = Filter(
        Clean(Shape(
            Stream(source, partition = f"event in ({ts})"), 
            evgroup
        ), notclean)
    )
    
    if console:
        log.info("Streamming to console")
        return SinkToConsole(stream)
    
    if debug:
        log.info("Streamming to SS using foreach")
        return SinkForeachToSS(stream, table)
    
    log.info("Streamming to SS")
    return SinkToSS(stream, table)
