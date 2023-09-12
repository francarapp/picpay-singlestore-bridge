from pyspark.sql.functions import col, length
from pyspark.sql.types import *
from core import session

from datetime import datetime, timedelta

import logging
log = logging.getLogger('core.extract.stream')

# "s3a://picpay-datalake-stream-landing/sparkstreaming/et/raw/track-events-approved/"

def Stream(file, partition="", printSchema=False):
    # dttm =  datetime.now() - timedelta(hours=1)
    # return createStream(file, dttm.strftime("%Y-%m-%d %H:%M:%S.%f")[:23], partition)
    strnow =  datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:10]
    start = f'{strnow} 00:00:00.000'
    log.info(f"Stream created with delta startingTimestamp: {start} to partition {partition}")
    return createStream(file, start, partition, printSchema)

def createStream(file, starting, partition, printSchema=False):
    stream = session.spark\
        .readStream.format("delta")\
            .option("maxBytesPerTrigger", 10485760)\
            .option("startingTimestamp", starting)
            #.option("startingVersion", '13444')
            #.option("startingTimestamp", '2023-06-29 00:00:00.000')
            #.option("startingTimestamp", starting)
    # if partition != "":
    #     stream = stream.option("partition", partition)    
    stream = stream.load(file)
    if partition != "":
        # stream = stream.where(partition)    
        stream = stream.filter(partition)
    log.info(f"Stream created from file {file} and partition {partition}")
    if printSchema:
        stream.printSchema()
    return stream

def SinkToS3(stream, evgroup):
    return stream.writeStream\
        .option("checkpointLocation", f"/home/spark/checkpoint/{evgroup}")\
        .option("path", f"s3a://picpay-dataeng-singlestore-landing/events/{evgroup}")\
        .partitionBy("ano","mes", "dia", "hora", "minuto")\
        .format("parquet")

    
def SinkToSS(stream, evgroup):
    def saveSS(df, epoch):
        
        df.drop('ano', 'mes', 'dia', 'hora', 'minuto')\
            .filter(length(col("properties")) < 10000)\
            .write.format("singlestore").mode("append")\
            .save(evgroup)
        
        if epoch%15 == 0 and log.isEnabledFor(logging.DEBUG):
            log.debug(f'DataFrame epoch:{"{:,}".format(epoch)} size: {"{:,}".format(df.count())}')
        
    return stream.writeStream.foreachBatch(saveSS)

def SinkToCsv(stream, evgroup):
    return stream.select('properties') \
	    .coalesce(1) \
	    .writeStream \
	    .option("checkpointLocation", f"/home/spark/checkpoint/{evgroup}") \
	    .option('path', f"/home/spark/json/{evgroup}") \
	    .trigger(processingTime="10 seconds") \
	    .format("csv") \
	    .outputMode("append") 

def SinkForeachToSS(stream, evgroup):
    schema = StructType([ \
        StructField("event_name",StringType(),True), \
        StructField("event_id",StringType(),True), \
        StructField("session_id",StringType(),True), \
        StructField("user_id", StringType(), True), \
        StructField("correlation_id", StringType(), True), \
        StructField("dt_created", StringType(), True), \
        StructField("dt_received", StringType(), True), \
        StructField("dt_bridged", StringType(), True), \
        StructField("context", StringType(), True), \
        StructField("properties", StringType(), True), \
        StructField("ano", StringType(), True), \
        StructField("mes", StringType(), True), \
        StructField("dia", StringType(), True), \
        StructField("hora", StringType(), True), \
        StructField("minuto", StringType(), True), \
    ])
    class ForeachWriter:
        def open(self, partition_id, epoch_id):
            # Open connection. This method is optional in Python.
            pass

        def process(self, row):
            log.debug(\
                f"Processing: event_name:{row[event_name]} event_id:{row[event_id]} session_id:{row[session_id]}" +\
                f"user_id: {row[user_id]} correlation_id: {row[correlation_id]} "+ \
                f"dt_created: {row[dt_created]} dt_received: {row[dt_received]} dt_bridged: {row[dt_bridged]} \n"+ \
                f"CONTEXT {row[context]}\n PAYLOAD {row[properties]} \n"
            )
            df = session.spark.createDataFrame([row], schema)
            df.drop('ano', 'mes', 'dia', 'hora', 'minuto')\
                .filter(length(col("properties")) < 10000)\
                .write.format("singlestore").mode("append")\
                .save(evgroup)

        def close(self, error):
           log.error(f"Erro ao persistir {error}")
            
      
    return stream.writeStream.foreach(ForeachWriter())


def SinkToConsole(stream):
    return stream.writeStream.format("console")
