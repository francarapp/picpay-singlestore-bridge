from pyspark.sql.functions import mean, stddev, max, min, sum, count, col, randn, round, to_date, date_format, percentile_approx, lit
from pyspark.sql.functions import when, coalesce

import logging
log = logging.getLogger('core.transform.columns')

def withEventName(df, name):
    columns =  (col for col in df.columns)
    if "event_name" in columns:
        return df
    elif "former_event_name" in columns:
        log.debug("Using column former_event_name as event_name")
        return df.withColumn('event_name', col('former_event_name'))
    elif "event" in columns:
        log.debug("Using column event as event_name")
        return df.withColumn('event_name', col('event'))
    
    log.debug(f"Using  value lit {name} as event_name")
    return df.withColumn('event_name', lit(name))

