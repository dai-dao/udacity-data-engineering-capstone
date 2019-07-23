import boto3
import configparser
import os

#
config = configparser.ConfigParser()
config.read('dwh.cfg')
os.environ["AWS_ACCESS_KEY_ID"] = config.get("CREDENTIALS", "AWS_ACCESS_KEY_ID")
os.environ["AWS_SECRET_ACCESS_KEY"] = config.get("CREDENTIALS", "AWS_SECRET_ACCESS_KEY")

#
s3 = boto3.client('s3')
filename = 'test.txt'
bucket_name = 'de-capstone'

#
root = "../../data/18-83510-I94-Data-2016/"
files = [root+f for f in os.listdir(root)]
for f in files:
    s3.upload_file(f, bucket_name, bucket_name + "/raw/i94_immigration_data/" + f.split("/")[-1])
    
#
s3.upload_file("data/us-cities-demographics.csv", bucket_name, 
               bucket_name + "/raw/demographics/us-cities-demographics.csv")

#
s3.upload_file("../../data2/GlobalLandTemperaturesByCity.csv", bucket_name, 
               bucket_name + "/raw/temperatures/GlobalLandTemperaturesByCity.csv")

# 
s3.upload_file("data/state_code.csv", bucket_name,
               bucket_name + "/raw/codes/state_code.csv")
s3.upload_file("data/country_code.csv", bucket_name,
               bucket_name + "/raw/codes/country_code.csv")
s3.upload_file("data/airport_code.csv", bucket_name,
               bucket_name + "/raw/codes/airport_code.csv")