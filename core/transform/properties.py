from pyspark.sql.functions import map_filter

def withProperties(df, columns):
    return df.withColumn("properties", map_filter(
        "properties", lambda k, v: k.isin(columns))
    )
