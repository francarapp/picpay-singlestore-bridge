from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from core import session
from bridge import Bridge
from datetime import datetime, timedelta

def main():
    ssc = StreamingContext(session.context, 1)
    dttm = datetime.now() - timedelta(hours=1)
    stream = Bridge("screen", "screen", dttm.strftime("%Y-%m-%d %H:%M:%S.%f")[:23])
    stream = stream.start()
    stream.awaitTermination()


if __name__ == "__main__":
    print("INICIANDO BRIDGE SCREEN")
    main()
