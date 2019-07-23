from pyspark.sql.types import *
from pyspark.sql.functions import udf, col
import pyspark.sql.functions as F
from pyspark.sql.types import *
import math
from datetime import datetime, timedelta
import sys
Logger = spark._jvm.org.apache.log4j.Logger
mylogger = Logger.getLogger(__name__)


def to_datetime(x):
    try:
        start = datetime(1960, 1, 1)
        return start + timedelta(days=int(x))
    except:
        return None
udf_to_datetime_sas = udf(lambda x: to_datetime(x), DateType())


def to_datetimefrstr(x):
    try:
        return datetime.strptime(x, '%m%d%Y')
    except:
        return None
udf_to_datetimefrstr = udf(lambda x: to_datetimefrstr(x), DateType())


immigrant = spark.read.format('com.github.saurfang.sas.spark')\
                 .load('s3://de-capstone/raw/i94_immigration_data/i94_{}_sub.sas7bdat'.format(month_year))\
                 .selectExpr('cast(cicid as int) AS cicid', 'cast(i94res as int) AS from_country_code', 
                             'cast(i94bir as int) AS age', 'cast(i94visa as int) AS visa_code', 
                             'visapost AS visa_post', 'occup AS occupation', 
                             'visatype AS visa_type', 'cast(biryear as int) AS birth_year', 'gender')\
                 .withColumn("i94_dt", F.lit(month_year))
#
immigrant.write.partitionBy("i94_dt").mode("append").parquet("s3://de-capstone/lake/immigrant/")


#
immigration = spark.read.format('com.github.saurfang.sas.spark')\
                     .load('s3://de-capstone/raw/i94_immigration_data/i94_apr16_sub.sas7bdat')\
                     .selectExpr('cast(cicid as int) cicid', 'arrdate', 'i94port AS iata_code', 
                                 'i94addr AS state_code', 'depdate', 'dtaddto',
                                 'airline', 'cast(admnum as long) AS admnum', 'fltno', 'entdepa', 
                                 'entdepd', 'entdepu', 'matflag')\
                     .withColumn("arrival_date", udf_to_datetime_sas("arrdate"))\
                     .withColumn("departure_date", udf_to_datetime_sas("depdate"))\
                     .withColumn("deadline_departure", udf_to_datetimefrstr("dtaddto"))\
                     .drop("arrdate", "depdate", "dtaddto")\
                     .withColumn("i94_dt", F.lit(month_year))
immigration.write.partitionBy("i94_dt").mode("append").parquet("s3://de-capstone/lake/immigration/")