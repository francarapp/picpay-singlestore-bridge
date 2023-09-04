
def getValueAsDF(df, properties, key):
    return df.rdd.map(lambda row: (row.__getitem__(properties)[key], )).toDF(['value'])
