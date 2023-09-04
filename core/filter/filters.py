from pyspark.sql.functions import col

def Filter(df):
    return filterYear(
            filterNull( 
                filterNull(df, "user_id"), 
            "event_id"), 
        "2023")

def filterYear(df, year):
    return df.filter(col('ano') >= year)

def filterNull(df, column):
    return df.filter(col(column).isNotNull())
