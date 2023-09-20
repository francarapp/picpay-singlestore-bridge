
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import col


def reencode(df):
	df = df.withColumn('properties', regexp_replace('properties', 
    	'[^[\d\w !@#\$%&\*\(\)_=\-\+\"\[\{\]\}~\^/\?;:\.>\,<çâêôâêôáéí]]', ''))
	df = df.withColumn('properties', regexp_replace('properties', 
    	'\'', ''))
	return df

def Clean(stream):
    return replaceDuplasAspas(
        replaceAspasColchetes(
            replaceAspasParenteses(
                stream
            )
        )
    )

def replaceAspasParenteses(df):
    return df.withColumn("properties", F.regexp_replace('properties', '\:\s*\"\s*\[', ':[')) \
        .withColumn("properties", F.regexp_replace('properties', '\]\s*\"\s*,', '],')) \
        .withColumn("properties", F.regexp_replace('properties', '\]\s*\"\s*}', ']}')) 


def replaceAspasColchetes(df):
    return df.withColumn("properties", F.regexp_replace('properties', '\:\s*\"\s*\{', ':{')) \
        .withColumn("properties", F.regexp_replace('properties', '\}\s*\"\s*,', '},')) \
        .withColumn("properties", F.regexp_replace('properties', '\}\s*\"\s*}', '}}')) 


def replaceDuplasAspas(df):
    return df.withColumn("properties", F.regexp_replace('properties', ':\s*\"\"\s*[^,]', ':\"'))\
        .withColumn("properties", F.regexp_replace('properties', '[^:]\s*\"\",', '\",')) 
