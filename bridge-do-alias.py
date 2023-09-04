from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from core import session
from bridge import Bridge
from datetime import datetime, timedelta

import logging

def conf():
    logging.basicConfig(format='[%(levelname)s] %(asctime)s {%(module)s} - %(message)s', level=logging.INFO)
    logging.getLogger("core.extract.stream").setLevel(logging.DEBUG)
    logging.getLogger('pyspark').setLevel(logging.ERROR)
    logging.getLogger("py4j").setLevel(logging.ERROR)

def main():
    stream = Bridge("alias", "alias")
    stream = stream.start()
    stream.awaitTermination()


if __name__ == "__main__":
    conf()
    main()
