from pyspark.sql.functions import mean, stddev, max, min, sum, count, col, randn, round, to_date, date_format, percentile_approx, lit
from pyspark.sql.functions import when, coalesce

import logging
log = logging.getLogger('core.transform.columns')

def withEventName(df, name):
    if "event_name" in df.columns:
        return df
    elif "name" in df.columns:
        log.debug("Using column name as event_name")
        return df.withColumn('event_name', col('name'))
    elif "event" in df.columns:
        log.debug("Using column event as event_name")
        return df.withColumn('event_name', col('event'))
    
    log.debug(f"Using  value lit {name} as event_name")
    return df.withColumn('event_name', lit(name))

