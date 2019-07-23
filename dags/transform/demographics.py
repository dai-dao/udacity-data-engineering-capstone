from pyspark.sql.types import *
from pyspark.sql.functions import udf, col
import pyspark.sql.functions as F
from pyspark.sql.types import *


demo = spark.read.format('csv').load('s3://de-capstone/raw/demographics/us-cities-demographics.csv', header=True, inferSchema=True, sep=';')\
                                .withColumn("male_population", col("Male Population").cast(LongType()))\
                                .withColumn("female_population", col("Female Population").cast(LongType()))\
                                .withColumn("total_population", col("Total Population").cast(LongType()))\
                                .withColumn("num_veterans", col("Number of Veterans").cast(LongType()))\
                                .withColumn("foreign_born", col("Foreign-born").cast(LongType()))\
                                .withColumnRenamed("Average Household Size", "avg_household_size")\
                                .withColumnRenamed("State Code", "state_code")\
                                .withColumnRenamed("Race","race")\
                                .withColumnRenamed("Median Age", "median_age")\
                                .withColumnRenamed("City", "city")\
                                .drop("State", "Count", "Male Population", "Female Population", 
                                      "Total Population", "Number of Veterans", "Foreign-born")
#
city = spark.read.parquet("s3://de-capstone/lake/city/")
#
demo = demo.join(city, (demo.city == city.city) & (demo.state_code == city.state_code))\
           .drop("city", "state_code")
#
demo.write.mode("overwrite").parquet("s3://de-capstone/lake/us_cities_demographics/")