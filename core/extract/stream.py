from pyspark.sql.functions import col, length
from pyspark.sql.types import *
from core import session

from datetime import datetime, timedelta

import logging
log = logging.getLogger('core.extract.stream')

# "s3a://picpay-datalake-stream-landing/sparkstreaming/et/raw/track-events-approved/"

def Stream(file, partition=""):
    dttm =  datetime.now() - timedelta(hours=1)
    return createStream(file, dttm.strftime("%Y-%m-%d %H:%M:%S.%f")[:23], partition)

def createStream(file, starting, partition):
    stream = session.spark\
        .readStream.format("delta")\
            .option("maxBytesPerTrigger", 10485760)\
            .option("startingTimestamp", starting)
    if partition != "":
        stream.option("partition", partition)
    stream = stream.load(file)
    
    log.info(f"Stream created from file {file}")
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
            df = spark.createDataFrame([row], schema)
            df.drop('ano', 'mes', 'dia', 'hora', 'minuto')\
                .filter(length(col("properties")) < 10000)\
                .write.format("singlestore").mode("append")\
                .save(evgroup)

        def close(self, error):
           log.error(f"Erro ao persistir {error}")
            
      
    return stream.writeStream.foreach(ForeachWriter()).start()


def SinkToConsole(stream):
    return stream.writeStream.format("console")
