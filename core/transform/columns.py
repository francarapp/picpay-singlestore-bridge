from pyspark.sql.functions import mean, stddev, max, min, sum, count, col, randn, round, to_date, date_format, percentile_approx, lit
from pyspark.sql.functions import when, coalesce

def withName(df, name):
    if "name" in (col for col in df.columns):
        return df
    elif "former_event_name" in (col for col in df.columns):
        return df.withColumn('name', col('former_event_name'))
    return df.withColumn('name', lit(name))

