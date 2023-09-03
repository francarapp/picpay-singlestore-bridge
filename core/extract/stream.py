from core import session

import datetime

# "s3a://picpay-datalake-stream-landing/sparkstreaming/et/raw/track-events-approved/"

def Stream(file):
    return createStream(file, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))

def createStream(file, starting):
    return session.spark\
        .readStream.format("delta")\
            .option("maxBytesPerTrigger", 10485760)\
            .option("startingTimestamp", starting) \
        .load(file)

def Sink(s3):
    #.partitionBy("mes", "dia", "hora", "min", "event_name")
    return df.writeStream \
        .option("path", s3) \
        .mode("overwrite") \
        .format('parquet')

    
def writeStream(df, format):
    return df.writeStream.format(format)
