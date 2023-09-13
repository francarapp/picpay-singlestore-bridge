from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from core import session
from bridge import Bridge
from conf import initlog

from datetime import datetime, timedelta

def conf():
    initlog()

def main():
    stream = Bridge("screen", "event_screen")
    stream = stream.start()
    stream.awaitTermination()


if __name__ == "__main__":
    conf()
    main()
