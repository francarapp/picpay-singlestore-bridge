from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from core import session
from bridge import BridgeInnerEvents
from conf import initlog

from datetime import datetime, timedelta

def conf():
    initlog()
    

def main():
    stream = BridgeInnerEvents("track", "event_business_interaction", evgroup="business", events=[
        "money_moved_approved",
        "money_moved"
    ], console=False)
    stream = stream.start()
    stream.awaitTermination()


if __name__ == "__main__":
    conf()
    main()
