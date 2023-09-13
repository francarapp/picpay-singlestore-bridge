from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from core import session
from bridge import BridgeTransactions, BridgeUnion
from conf import initlog

from datetime import datetime, timedelta

def conf():
    initlog()
    

def main():
    stream = BridgeUnion("track", "event_transaction", transactions=[
        "transaction_accomplished",
        "transaction_delayed_approved",
        "transaction_invoiced",
        "button_viewed"
    ], console=True)
    stream = stream.start()
    stream.awaitTermination()


if __name__ == "__main__":
    conf()
    main()
