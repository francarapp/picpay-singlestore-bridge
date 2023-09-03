from core import session

from operator import add

def reduceByWord(file):
    return session.spark.read.text(file).rdd.map(lambda r: r[0]) \
        .flatMap(lambda x: x.split(' ')) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add)