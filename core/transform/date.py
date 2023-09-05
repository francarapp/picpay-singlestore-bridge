from pyspark.sql.functions import from_utc_timestamp, to_utc_timestamp, date_format
from pyspark.sql.functions import regexp_replace, regexp_extract
from pyspark.sql.functions import col

def withDate(df, column):
    df = df \
    .withColumn(column+"_tz", 
            regexp_extract(column, "([-+]\d{2,4}$)", 1)
        ) \
    .withColumn(column+"_ts", 
        regexp_replace(
            regexp_replace(column, "T", " "), \
            "[-+]\d{2,4}$", "")\
    )

    return df.withColumn( 
            column,
            date_format(
                from_utc_timestamp(to_utc_timestamp(col(column+"_ts"), col(column+"_tz")), "-0300"), \
                "yyyy-MM-dd HH:mm:ss.SSS"
            )
    )

def withTimeslice(df):
    return df.withColumn('ano', date_format(col('dt_created'), 'yyyy')) \
        .withColumn('mes', date_format(col('dt_created'), 'MM')) \
        .withColumn('dia', date_format(col('dt_created'), 'dd')) \
        .withColumn('hora', date_format(col('dt_created'), 'HH')) \
        .withColumn('minuto', date_format(col('dt_created'), 'mm')) 