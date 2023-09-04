from core import session

import datetime

# "s3a://picpay-datalake-stream-landing/sparkstreaming/et/raw/track-events-approved/"

def Stream(file):
    dttm =  datetime.now() - datetime.timedelta(hours=1)
    return createStream(file, dttm.strftime("%Y-%m-%d %H:%M:%S.%f")[:23])

def createStream(file, starting):
    return session.spark\
        .readStream.format("delta")\
            .option("maxBytesPerTrigger", 40485760)\
            .option("startingTimestamp", starting) \
        .load(file)

def SinkToS3(stream, evgroup):
    return stream.writeStream\
        .option("checkpointLocation", f"/home/spark/checkpoint/{evgroup}")\
        .option("path", f"s3a://picpay-dataeng-singlestore-landing/events/{evgroup}")\
        .partitionBy("ano","mes", "dia", "hora", "minuto")\
        .format("parquet")

    
def SinkToSS(stream, evgroup):
    def saveSS(df, epoch):
        df.drop('ano', 'mes', 'dia', 'hora', 'minuto').write.format("singlestore").mode("append").save(evgroup)
        
    return stream.writeStream.foreachBatch(saveSS)