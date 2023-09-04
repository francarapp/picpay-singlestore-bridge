from pyspark.sql.function import col

def filterYear(df, year):
    return df.filter(col('ano') >= year)