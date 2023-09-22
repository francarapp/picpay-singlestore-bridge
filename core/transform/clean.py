
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import col, when


def reencode(df):
	df = df.withColumn('properties', regexp_replace('properties', 
    	'[^[\d\w !@#\$%&\*\(\)_=\-\+\"\[\{\]\}~\^/\?;:\.>\,<çâêôâêôáéí]]', ''))
	df = df.withColumn('properties', regexp_replace('properties', 
    	'\'', ''))
	return df

def Clean(stream, excludesColchetes=[], excludesChaves=[]):
    return replaceAspasColchetes(
            replaceAspasParenteses(
                replaceDuplasBarras(
                    stream
                ), excludes=excludesColchetes
            ), excludes=excludesChaves
        )
    

def replaceAspasParenteses(df, excludes=[]):
    return df\
        .withColumn("properties", 
            when(
                ~col('event_name').isin(excludes), 
                F.regexp_replace('properties', '\:\s*\"\s*\[', ':[')
            ).otherwise(col('properties')))\
        .withColumn("properties", 
            when(
                ~col('event_name').isin(excludes), 
                F.regexp_replace('properties', '\]\s*\"\s*,', '],')
            ).otherwise(col('properties')))\
        .withColumn("properties", 
            when(
                ~col('event_name').isin(excludes), 
                F.regexp_replace('properties', '\]\s*\"\s*}', ']}')
            ).otherwise(col('properties')))    


def replaceAspasColchetes(df, excludes=[]):
    return df\
        .withColumn("properties", 
            when(
                ~col('event_name').isin(excludes), 
                F.regexp_replace('properties', '\:\s*\"\s*\{', ':{')
            ).otherwise(col('properties')))\
        .withColumn("properties", 
            when(
                ~col('event_name').isin(excludes), 
                F.regexp_replace('properties', '\}\s*\"\s*,', '},')                
            ).otherwise(col('properties')))\
        .withColumn("properties", 
            when(
                ~col('event_name').isin(excludes), 
                F.regexp_replace('properties', '\}\s*\"\s*}', '}}')
            ).otherwise(col('properties')))


def replaceDuplasAspas(df):
    return\
        df.withColumn("properties", 
            F.regexp_replace('properties', ':\s*\"\"\s*[^,]', ':\"'))\
        .withColumn("properties", 
            F.regexp_replace('properties', '[^:]\s*\"\",', '\",'))

def replaceDuplasBarras(df):
    return df.withColumn('properties', 
            F.regexp_replace('properties', '(\\\)*"', '"'))
