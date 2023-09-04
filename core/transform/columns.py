from pyspark.sql.functions import mean, stddev, max, min, sum, count, col, randn, round, to_date, date_format, percentile_approx, lit
from pyspark.sql.functions import when, coalesce

def withName(df, name):
    columns =  (col for col in df.columns)
    if "name" in columns:
        return df
    elif "former_event_name" in columns:
        return df.withColumn('name', col('former_event_name'))
    elif "event" in columns:
        return df.withColumn('name', col('event'))
    elif "event_name" in columns:
        return df.withColumn('name', col('event_name'))    
    return df.withColumn('name', lit(name))

