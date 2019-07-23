from pyspark.sql.functions import col
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import udf


def parse_lat(x):
    y = x.strip().split(',')
    return float(y[0])
udf_parse_lat = udf(lambda x: parse_lat(x), FloatType())

def parse_long(x):
    y = x.strip().split(',')
    return float(y[1])
udf_parse_long = udf(lambda x: parse_long(x), FloatType())

def parse_state(x):
    return x.strip().split('-')[-1]
udf_parse_state = udf(lambda x: parse_state(x), StringType())


#
spark.read.format('csv').load('s3://de-capstone/raw/codes/state_code.csv', header=True, inferSchema=True)\
                        .write.mode("overwrite").parquet("s3://de-capstone/lake/codes/state_code/")

#
spark.read.format('csv').load('s3://de-capstone/raw/codes/country_code.csv', header=True, inferSchema=True, sep=';')\
                        .write.mode("overwrite").parquet("s3://de-capstone/lake/codes/country_code/")
                
#
city = spark.read.parquet("s3://de-capstone/lake/city/")
us_airport = spark.read.format('csv').load('s3://de-capstone/raw/codes/airport_code.csv', header=True, inferSchema=True)\
                        .withColumn("airport_latitude", udf_parse_lat("coordinates"))\
                        .withColumn("airport_longitude", udf_parse_long("coordinates"))\
                        .filter("iso_country = 'US'")\
                        .withColumn("state", udf_parse_state("iso_region"))\
                        .withColumnRenamed("ident", "icao_code")\
                        .drop("coordinates", "gps_code", "local_code", "continent", 
                                "iso_region", "iso_country")
us_airport = us_airport.join(city, (us_airport.municipality==city.city) & (us_airport.state==city.state_code), "left")\
                        .drop("municipality", "state", "city", "state_code")
us_airport.write.mode("overwrite").parquet("s3://de-capstone/lake/codes/airport_code/")