from core import session

# "s3a://picpay-datalake-stream-landing/sparkstreaming/et/raw/track-events-approved/"
def createStream(file, starting):
    return session.spark\
        .readStream.format("delta")\
            .option("maxBytesPerTrigger", 10485760)\
            .option("startingTimestamp", starting) \
        .load(file)
        
def writeStream(df, format):
    return df.writeStream.format(format)
