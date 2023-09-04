from pyspark.sql.functions import mean, stddev, max, min, sum, count, col, randn, round, to_date, date_format, percentile_approx, lit
from pyspark.sql.functions import when, coalesce
from pyspark.sql.functions import to_json

from .date import withDate, withTimeslice

import datetime

def withReshape(df, evname):
    if "name" in (col for col in df.columns):
        df = df.withColumnRenamed('name', 'event_name')
    else:
        df = df.withColumn('event_name', lit(evname))
    df = df.withColumn("userId",
        when(
            col("userId").isNull(), col("anonymousId")
        ).otherwise(col("userId"))
)
    return df \
    .withColumnRenamed('uuid', 'event_id') \
    .withColumnRenamed('userId', 'user_id') \
    .withColumnRenamed('createdAt', 'dt_created') \
    .withColumnRenamed('sendAt', 'dt_received') \
    .withColumn('dt_bridged',  lit(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:23])) \
    .withColumn(
            "session_id",
            coalesce(
                col("context").getItem("session_id"),
                lit(None)
            )) \
    .withColumn(
            "correlation_id",
            coalesce(
                col("context").getItem("correlation_id"),
                lit(None)
            )) \
    .withColumn('context', to_json(col('context'))) \
    .withColumn('properties', to_json(col('properties'))) 

def Shape(df, name="UNDEFINED"):
    return withTimeslice(withDate(
        withDate(
            withReshape(df, name), 
            'dt_created'
        ), 'dt_received'
        )).select(
            'ano', 'mes', 'dia', 'hora', 'minuto', 'event_name', 
            'event_id', 'session_id', 'user_id', 'correlation_id', 
            'dt_created', 'dt_received', 'dt_bridged', 
            'context', 'properties'
    )   
