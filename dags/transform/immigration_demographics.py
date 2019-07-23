from pyspark.sql.types import *
from pyspark.sql.functions import udf, col
import pyspark.sql.functions as F
from pyspark.sql.types import *
import math
from datetime import datetime, timedelta


immigrant = spark.read.parquet("s3://de-capstone/lake/immigrant/")\
                      .filter("i94_dt = '{}'".format(month_year))
country_code = spark.read.parquet("s3://de-capstone/lake/codes/country_code/")
state_code = spark.read.parquet("s3://de-capstone/lake/codes/state_code/")
immigration = spark.read.parquet("s3://de-capstone/lake/immigration/")\
                        .filter("i94_dt = '{}'".format(month_year))
city = spark.read.parquet("s3://de-capstone/lake/city/")
demo = spark.read.parquet("s3://de-capstone/lake/us_cities_demographics/")\
            .select("median_age", "city_id", "total_population", "foreign_born")\
            .join(city.select("state_code", "city_id"), "city_id")\
            .drop("city_id")\
            .groupBy("state_code").agg(F.mean("median_age").alias("median_age"), 
                                       F.sum("total_population").alias("total_population"),
                                       F.sum("foreign_born").alias("foreign_born"))


immigration_demographic = immigrant.select("cicid", "from_country_code", "age", "occupation", "gender", 'i94_dt')
immigration_demographic = immigration_demographic.join(country_code, immigration_demographic.from_country_code==country_code.code, "left")\
                                                .drop("from_country_code", "code")\
                                                .withColumnRenamed("country", "from_country")
immigration_demographic = immigration_demographic.join(immigration.select("cicid", "state_code"), "cicid", "left")
immigration_demographic = immigration_demographic.join(state_code, immigration_demographic.state_code==state_code.code, "left")\
                                                .drop("code")
immigration_demographic = immigration_demographic.join(demo, "state_code").drop("state_code")
immigration_demographic.write.partitionBy("i94_dt").mode("append")\
                       .parquet("s3://de-capstone/lake/immigration_demographic/")