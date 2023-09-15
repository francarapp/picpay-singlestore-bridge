import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import mean, stddev, max, min, sum, count, col, randn, round, to_date, date_format, percentile_approx, lit
from pyspark.sql.functions import when, coalesce
from pyspark.sql.functions import  create_map
from pyspark.sql.functions import to_json, from_json
from itertools import chain

from .date import withDate, withTimeslice
from .columns import withEventName
from .properties import reshapeProperties

from datetime import datetime, timezone, timedelta

def withReshape(df, evgroup, evname): 
    df = withEventName(df, evname) \
        .withColumnRenamed('uuid', 'event_id') \
        .withColumnRenamed('userId', 'user_id') \
        .withColumnRenamed('createdAt', 'dt_created') \
        .withColumnRenamed('sendAt', 'dt_received') \
        .withColumn('dt_bridged',  lit(datetime.now(timezone(timedelta(hours=-3.0))).strftime("%Y-%m-%d %H:%M:%S.%f")[:23])) \
        .withColumn(
            "session_id",
            coalesce(
                col("context").getItem("session_id"),
                lit(None)
            ))
                   
    match evgroup:
        case 'alias':
            df = df \
                .withColumn('properties', lit(None))\
                .withColumn('correlation_id', 
                    coalesce(
                        col('context').getItem('correlation_id'),\
                        lit(None)
                    )\
                )
        case 'identify':
            df = df \
                .withColumn('properties', lit(None))\
                .withColumn('correlation_id', 
                    coalesce(
                        col('context').getItem('correlation_id'),\
                        lit(None)
                    )\
                )
        case 'interaction':
            df = reshapeProperties(
                df.withColumn('correlation_id', 
                    coalesce(
                        col('properties').getItem('correlation_id'),\
                        col('context').getItem('correlation_id'),\
                        lit(None)
                    )\
                    ) 
                )
        case 'business':
            df = df.withColumn('correlation_id', 
                    coalesce(
                        col('properties').getItem('correlation_id'),\
                        col('context').getItem('correlation_id'),\
                        lit(None)
                    ))
            
        case other:
            df = df\
                .withColumn('correlation_id', 
                    coalesce(
                        col('properties').getItem('correlation_id'),\
                        col('context').getItem('correlation_id'),\
                        from_json(col('properties').getItem('transaction'), schemaTransaction).getItem('correlation_id'),\
                        lit(None)
                    )\
                )\
                .withColumn("user_id",
                    coalesce(
                        col("user_id"),
                        col("anonymousId")
                    )
                ) 
    return withRetype(df)

def withRetype(df):
    return df\
        .withColumn('properties', 
            when( to_json(col('properties')) != "", to_json(col('properties')) ) \
            .otherwise(lit(None))
        )\
        .withColumn('context',  to_json(col('context')))

      
def preparePropertiesForSelect(evgroup):
    match evgroup:
        case 'identify':            
            properties = \
                to_json(
                    create_map(list(chain(*(
                        [
                            (lit('brazeConfiguration'), col('brazeConfiguration')), 
                            (lit('traits'), col('traits'))
                        ]
                    ))))
                ).alias("properties")
            return properties
        case 'alias':
            properties = to_json(create_map(list(chain(*(
                [
                    (lit('previousId'), col('previousId'))
                ]
            ))))).alias("properties")
            return properties
        case other:
            return 'properties'      

def Shape(df, evgroup, evname=None):
    evname = evname if evname is not None else evgroup
    return withTimeslice(withDate(
        withDate(
            withReshape(df, evgroup, evname), 
            'dt_created'
        ), 'dt_received'
        )).select(
            'ano', 'mes', 'dia', 'hora', 'minuto', 'event_name', 
            'event_id', 'session_id', 'user_id', 'correlation_id', 
            'dt_created', 'dt_received', 'dt_bridged', 
            'context', preparePropertiesForSelect(evgroup)
        )   

schemaTransaction = StructType([
    StructField("id", StringType()),
    StructField("correlation_id", StringType())
])


    # StructField("product", StringType()),
    # StructField("profile", StringType()),
    # StructField("status", StringType()),
    # StructField("payer_id", StringType()),
    # StructField("payer_type", StringType()),
    # StructField("base_value", StringType()),
    # StructField("payer_value", StringType()),
    # StructField("receiver_value", StringType()),
    # StructField("updated_at", StringType()),
    # StructField("created_at", StringType())