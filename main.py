from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, udf, first
from pyspark.sql.types import StringType
import pygeohash as pgh
import requests
import time
import glob
from functools import reduce

RESTAURANT_PATH = "restaurant_csv/"
WEATHER_ROOT = "artifacts"
OUTPUT_PATH = "output/enriched_restaurants"

OPENCAGE_KEY = ""
OPENCAGE_URL = "https://api.opencagedata.com/geocode/v1/json"

def geocode_address(query: str):
    if not query:
        return None, None
    try:
        resp = requests.get(OPENCAGE_URL, params={"q": query, "key": OPENCAGE_KEY, "limit": 1, "no_annotations": 1}, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data["results"]:
            geo = data["results"][0]["geometry"]
            return geo["lat"], geo["lng"]
        return None, None
    except:
        return None, None

def geohash_4(lat, lng):
    if lat is None or lng is None:
        return None
    return pgh.encode(lat, lng, precision=4)

geohash_udf = udf(geohash_4, StringType())

def read_restaurants(spark, path):
    return spark.read.option("header","true").option("inferSchema","true").csv(path)

def read_weather(spark, root_path):
    paths = glob.glob(f"{root_path}/weather *")
    dfs = [spark.read.parquet(p) for p in paths]
    return reduce(lambda a,b: a.unionByName(b), dfs)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("RestaurantWeatherETL").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    restaurants_df = read_restaurants(spark, RESTAURANT_PATH)
    weather_df = read_weather(spark, WEATHER_ROOT)

    missing_df = restaurants_df.filter(col("lat").isNull() | col("lng").isNull()).select("id", "city", "country").distinct()

    resolved_rows = []
    for r in missing_df.collect():
        query = f"{r.city}, {r.country}" if r.city and r.country else ""
        lat, lng = geocode_address(query)
        if lat is None or lng is None:
            lat, lng = 0.0, 0.0
        resolved_rows.append(Row(id=r.id, lat=lat, lng=lng))
        time.sleep(0.1)

    if resolved_rows:
        resolved_df = spark.createDataFrame(resolved_rows)
        restaurants_df = restaurants_df.drop("lat","lng").join(resolved_df, on="id", how="left")

    restaurants_geo = restaurants_df.withColumn("geohash", geohash_udf(col("lat"), col("lng")))
    weather_geo = weather_df.withColumn("geohash", geohash_udf(col("lat"), col("lng")))

    weather_dedup = weather_geo.groupBy("geohash").agg(
        first("avg_tmpr_c").alias("avg_tmpr_c"),
        first("avg_tmpr_f").alias("avg_tmpr_f"),
        first("wthr_date").alias("wthr_date")
    )

    enriched_df = restaurants_geo.join(weather_dedup, on="geohash", how="left")

    enriched_df.write.mode("overwrite").partitionBy("geohash").parquet(OUTPUT_PATH)

    print("Restaurant count:", restaurants_df.count())
    print("Enriched count:", enriched_df.count())
    enriched_df.show(5)
