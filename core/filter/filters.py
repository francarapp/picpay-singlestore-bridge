from pyspark.sql.functions import col, length

def Filter(df):
    return filterYear(
        filterLength(
            filterNull( 
                filterNull(df, "user_id"), 
            "event_id"), 
        "properties", 100000),
    "2023")

def filterYear(df, year):
    return df.filter(col('ano') >= year)

def filterNull(df, column):
    return df.filter(col(column).isNotNull())

def filterLength(df, column, size=10000000):
    return df.filter(length(col(column)) <= size)