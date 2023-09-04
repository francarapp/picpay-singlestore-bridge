from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from core import session
from bridge import Bridge
from datetime import datetime, timedelta

def main():
    stream = Bridge("screen", "screen")
    stream = stream.start()
    stream.awaitTermination()


if __name__ == "__main__":
    print("INICIANDO BRIDGE SCREEN")
    main()
