import os
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, DoubleType
from pyspark.sql.functions import pandas_udf
import time
import config

from geo_utils import geocode_restaurant, calculate_geohash, haversine_distance

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


spark = SparkSession \
    .builder \
    .master("local[*]") \
    .config("spark.io.compression.codec", "zstd") \
    .config("spark.sql.execution.pythonUDF.arrow.enabled", "true") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
    .config("spark.sol.shuffle.partitions", 200) \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.instances", "4") \
    .config("spark.executor.cores", "5") \
    .config("spark.sql.files.maxRecordsPerFile", 10_000) \
    .config("spark.debug.maxToStringFields", 1000) \
    .config("spark.executor.heartbeatInterval", "3600s") \
    .config("spark.network.timeout", "7200s") \
    .config("spark.network.timeoutInterval", "3600s") \
    .enableHiveSupport() \
    .getOrCreate()

weather_dir = config.WEATHER_DIR + '/weather'
restaurant_dir = config.RESTAURANT_DIR

restaurants_df = spark.read.csv(restaurant_dir, header=True)
weather_df = spark.read.parquet(weather_dir)

geocode_udf = pandas_udf(f=geocode_restaurant, returnType='lat double, lng double')
geohash_udf = pandas_udf(f=calculate_geohash, returnType=StringType())
haversine_udf = pandas_udf(f=haversine_distance, returnType=DoubleType())

geocoded_df = (
    restaurants_df.where("lat is null or lng is null")
    .drop("lat", "lng")
    .select("*", geocode_udf("franchise_name", "country", "city").alias("lat_lng"))
    .select(
        "*",
        F.col("lat_lng").getField("lat").alias("lat"),
        F.col("lat_lng").getField("lng").alias("lng"),
    )
    .drop("lat_lng")
)

restaurants_df = (
    restaurants_df.where("lat is not null and lng is not null")
    .unionAll(geocoded_df)
    .select("*", geohash_udf("lat", "lng").alias("geohash"))
    .withColumn("lat", F.col("lat").cast(DoubleType()))
    .withColumn("lng", F.col("lng").cast(DoubleType()))
    .withColumnsRenamed({"lat": "restaurant_lat", "lng": "restaurant_lng"})
    .dropDuplicates(["restaurant_franchise_id", "geohash"])
)


weather_df = weather_df.select("*", geohash_udf("lat", "lng").alias("geohash")).withColumnsRenamed(
    {"lat": "weather_lat", "lng": "weather_lng"}
).dropDuplicates(["geohash", "year", "month", "day"])


enriched_df = (
    weather_df.join(restaurants_df, on=["geohash"], how="left")
    .where("restaurant_lat is not null")
    .dropDuplicates(["geohash", "year", "month", "day", "restaurant_franchise_id"])
)

enriched_df.explain()

# time.sleep(10_000)

(
    enriched_df
    .repartition(8)
    .write
    .partitionBy("year", "month", "day")
    .mode("overwrite").parquet("output/enriched")
)
