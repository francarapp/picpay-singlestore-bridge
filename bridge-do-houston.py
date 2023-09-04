from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from core import session
from bridge import Bridge

from datetime import datetime, timedelta
import logging

def conf():
    logging.basicConfig(format='[%(levelname)s] %(asctime)s - %(message)s', level=logging.INFO)
    
def main():
    stream = Bridge("houston", "houston")
    stream = stream.start()
    stream.awaitTermination()



if __name__ == "__main__":
    conf()
    main()
    