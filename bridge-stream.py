from core.extract import Stream, SinkToS3
from core.transform import  Shape

stream = Shape(Stream("s3a://picpay-datalake-stream-landing/sparkstreaming/et/raw/screen-events-approved/", "2023-08-09 22:30:00"))

allstream = stream.writeStream.option("checkpointLocation", "/home/spark/checkpoint").option("path", "s3a://picpay-dataeng-singlestore-landing/events/screen").format("parquet")

allstream.start()

