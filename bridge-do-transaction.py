from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from core import session
from bridge import BridgeTransactions
from conf import initlog

from datetime import datetime, timedelta

def conf():
    initlog()
    

def main():
    stream = BridgeTransactions("track", "event_transaction", transactions=[
        "transaction_accomplished",
        "transaction_delayed_approved"
    ], console=True)
    stream = stream.start()
    stream.awaitTermination()


if __name__ == "__main__":
    conf()
    main()
