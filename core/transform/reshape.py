from pyspark.sql.functions import mean, stddev, max, min, sum, count, col, randn, round, to_date, date_format, percentile_approx, lit
from pyspark.sql.functions import when, coalesce
from pyspark.sql.functions import  create_map
from pyspark.sql.functions import to_json
from itertools import chain

from .date import withDate, withTimeslice
from .columns import withEventName

import datetime

def withReshape(df, evname):
    df = withEventName(df, evname) \
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
        .withColumn('context',  to_json(col('context')))
        
    match evname:
        case 'alias':
            return df \
                .withColumn('properties', lit('{}'))\
                .withColumn('correlation_id', lit(None))
        case 'identify':
            return df \
                .withColumn('properties', lit('{}'))\
                .withColumn('correlation_id', lit(None))
        case other:
            return df\
                .withColumn('properties', 
                    when( to_json(col('properties')) != "", to_json(col('properties')) ) \
                    .otherwise(lit(None))
                )\
                .withColumn('correlation_id', 
                    when(
                        col('properties').getItem('correlation_id').isNotNull(), 
                        col('properties').getItem('correlation_id')
                    ).otherwise(
                        when(
                            col('context').getItem('correlation_id').isNotNull(),
                            col('context').getItem('correlation_id')
                        ).otherwise(lit(None))
                )\
                .withColumn("user_id",
                    when(
                        col("user_id").isNull(), col("anonymousId")
                    ).otherwise(col("user_id"))
                )                      
            ) 
 
       
def preparePropertiesForSelect(evgroup):
    match evgroup:
        case 'identify':            
            properties = create_map(list(chain(*(
                [
                    (lit('brazeConfiguration'), col('brazeConfiguration')), 
                    (lit('traits'), col('traits'))
                ]
            )))).alias("properties")
            return properties
        case 'alias':
            properties = create_map(list(chain(*(
                [
                    (lit('previousId'), col('previousId'))
                ]
            )))).alias("properties")
            return properties
        case other:
            return 'properties'      

def Shape(df, evgroup, name="UNDEFINED"):
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
