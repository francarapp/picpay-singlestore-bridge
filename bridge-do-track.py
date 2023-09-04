from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from core import session
from bridge import Bridge
from conf import args, initlog, partitionElements

from datetime import datetime, timedelta
import sys

def conf():
    initlog()
    args(sys.argv[1:])
    

def main():
    stream = Bridge("track", "track", partitions=partitionElements)
    stream = stream.start()
    stream.awaitTermination()


if __name__ == "__main__":
    conf()
    main()
