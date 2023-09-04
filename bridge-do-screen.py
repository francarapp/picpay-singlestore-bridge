from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from core import session
from bridge import Bridge
from datetime import datetime, timedelta

import logging

def conf():
    logging.basicConfig(format='[%(levelname)s] %(asctime)s - %(message)s', level=logging.INFO)
    logging.getLogger("core.extract.stream").setLevel(logging.DEBUG)

def main():
    stream = Bridge("screen", "screen")
    stream = stream.start()
    stream.awaitTermination()


if __name__ == "__main__":
    conf()
    main()
