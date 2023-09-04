from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from core import session
from bridge import Bridge
from conf import args, initlog

from datetime import datetime, timedelta

def conf():
    initlog()
    args()

def main():
    stream = Bridge("page", "page")
    stream = stream.start()
    stream.awaitTermination()


if __name__ == "__main__":
    conf()
    main()
