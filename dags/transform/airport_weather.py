us_airport = spark.read.parquet("s3://de-capstone/lake/codes/airport_code/")
city = spark.read.parquet("s3://de-capstone/lake/city/")
state_code = spark.read.parquet("s3://de-capstone/lake/codes/state_code/")
us_wea = spark.read.parquet("s3://de-capstone/lake/us_cities_temperatures/")


airport_weather = us_airport.select("name", "elevation_ft", "city_id")\
                            .join(city.select("city", "city_id", "state_code"), "city_id", "left")
airport_weather = airport_weather.join(state_code, airport_weather.state_code==state_code.code, "left")\
                                 .drop("state_code", "code")
airport_weather = airport_weather.join(us_wea, "city_id", "inner").drop("city_id")
airport_weather.write.mode("overwrite").parquet("s3://de-capstone/lake/us_airports_weather/")