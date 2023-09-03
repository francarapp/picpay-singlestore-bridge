from core import session


session.spark\
    .readStream.format("delta")\
            .option("maxBytesPerTrigger", 10485760)\
            .option("startingTimestamp", "2023-09-03 13:15") \
        .load("s3a://picpay-datalake-stream-landing/sparkstreaming/et/raw/track-events-approved/") \
    .writeStream.format("console").start() 