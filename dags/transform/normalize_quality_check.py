from pyspark.sql.functions import col
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import udf
spark.sparkContext.setLogLevel('WARN')

Logger= spark._jvm.org.apache.log4j.Logger
mylogger = Logger.getLogger("DAG")


def check(path, table):
    df = spark.read.parquet(path)
    if len(df.columns) > 0 and df.count() > 0:
        mylogger.warn("{} SUCCESS".format(table))
    else:
        mylogger.warn("{} FAIL".format(table))



check("s3a://de-capstone/lake/city/", "city")
check("s3a://de-capstone/lake/codes/state_code/", "state_code")
check("s3a://de-capstone/lake/codes/country_code/", "country_code")
check("s3a://de-capstone/lake/codes/airport_code/", "airport_code")
check("s3a://de-capstone/lake/us_cities_demographics/", "us_cities_demographics")
check("s3a://de-capstone/lake/us_cities_temperatures/", "us_cities_temperatures")
check("s3a://de-capstone/lake/us_airports_weather/", "us_airport_weather")