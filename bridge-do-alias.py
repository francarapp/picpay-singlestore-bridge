from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from core import session
from bridge import Bridge
from conf import args, initlog

from datetime import datetime, timedelta

def conf():
    initlog()
    
def main():
    stream = Bridge("alias", "event_alias")
    stream = stream.start()
    stream.awaitTermination()


if __name__ == "__main__":
    conf()
    main()
