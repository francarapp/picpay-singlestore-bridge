from pyspark.sql.functions import col

def filterYear(df, year):
    return df.filter(col('ano') >= year)
