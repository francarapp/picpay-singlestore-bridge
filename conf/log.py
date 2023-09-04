import logging
def initlog():
    logging.basicConfig(format='[%(levelname)s] %(asctime)s {%(module)s} - %(message)s', level=logging.INFO)
    logging.getLogger("core.transform.columns").setLevel(logging.DEBUG)
    logging.getLogger("core.extract.stream").setLevel(logging.DEBUG)
    logging.getLogger('pyspark').setLevel(logging.ERROR)
    logging.getLogger("py4j").setLevel(logging.ERROR)