from core.extract import Stream, SinkToSS, SinkToConsole, SinkForeachToSS
from core.transform import  Shape
from core.filter import Filter
from conf import partitionby, partitionEvName

import logging
log = logging.getLogger(__name__)

def Bridge(landing, table, partitions=[], console=False, debug=False):
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
            Stream(source, partition = partitionedby), 
            landing, evname
        )
    )
    
    if console:
        log.info("Streamming to console")
        return SinkToConsole(stream)
    
    if debug:
        log.info("Streamming to SS using foreach")
        return SinkForeachToSS(stream, table)
    
    log.info("Streamming to SS")
    return SinkToSS(stream, table)

def BridgeTransactions(landing, table, transactions=[], console=False, debug=False):
    ts = ','.join([f"'{t}'" for t in transactions])
    source = f's3a://picpay-datalake-stream-landing/sparkstreaming/et/raw/{landing}-events-approved/'
    
    log.info(f"Bridging from {source} to {table}")
    stream = Filter(
        Shape(
            Stream(source, partition = f"event in ({ts})"), 
            landing, 'transaction'
        )
    )
    
    if console:
        log.info("Streamming to console")
        return SinkToConsole(stream)
    
    if debug:
        log.info("Streamming to SS using foreach")
        return SinkForeachToSS(stream, table)
    
    log.info("Streamming to SS")
    return SinkToSS(stream, table)

def BridgeUnionTransactions(landing, table, transactions, console=False):
    stream = None
    for transact in transactions:
        partitionedby = f"event=='{transact}'"
        log.info(f"Using {landing} partition transaction {transact}")
        
        source = f's3a://picpay-datalake-stream-landing/sparkstreaming/et/raw/{landing}-events-approved/'
    
        log.info(f"Bridging {transact} transactions from {source} to {table}")
        transactStream = Filter(
            Shape(
                Stream(source, partition = partitionedby), 
                landing, transact
            )
        )        
        stream = stream.union(transactStream) if stream is not None else transactStream
    
    
    if console:
        log.info("Streamming to console")
        return SinkToConsole(stream)
    
    log.info("Streamming to SS")
    return SinkToSS(stream, table)
