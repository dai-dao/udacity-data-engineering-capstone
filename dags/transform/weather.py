from pyspark.sql.types import *
from pyspark.sql.functions import udf, col
import pyspark.sql.functions as F
from pyspark.sql.types import *


def convert_latitude(x):
    direction = str(x)[-1]
    if direction == 'N':
        return float(str(x)[:-1])
    else:
        return -float(str(x)[:-1])
udf_convert_latitude = udf(lambda x: convert_latitude(x), FloatType())

def convert_longitude(x):
    direction = str(x)[-1]
    if direction == 'E':
        return float(str(x)[:-1])
    else:
        return -float(str(x)[:-1])
udf_convert_longitude = udf(lambda x: convert_longitude(x), FloatType())


thres = F.to_date(F.lit("2013-08-01")).cast(TimestampType())
us_wea = spark.read.format('csv').load('s3://de-capstone/raw/temperatures/GlobalLandTemperaturesByCity.csv', header=True, inferSchema=True)\
                                 .where(col("dt") > thres)\
                                 .withColumn("latitude", udf_convert_latitude("Latitude"))\
                                 .withColumn("longitude", udf_convert_longitude("Longitude"))\
                                 .withColumnRenamed("AverageTemperature", "avg_temp")\
                                 .withColumnRenamed("AverageTemperatureUncertainty", "std_temp")\
                                 .withColumnRenamed("City", "city")\
                                 .withColumnRenamed("Country", "country")\
                                 .where(col("avg_temp").isNotNull())\
                                 .filter("country = 'United States'")\
                                 .drop("dt", "country", "latitude", "longitude")

# Replace city with city_id
city = spark.read.parquet("s3://de-capstone/lake/city/")
us_wea = us_wea.join(city, "city", "left").drop("city", "state_code", "city_latitude", "city_longitude")
us_wea.write.mode("overwrite").parquet("s3://de-capstone/lake/us_cities_temperatures/")