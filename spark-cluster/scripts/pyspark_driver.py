#!/usr/bin/env python3

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Mac driver test") \
    .master("spark://192.168.0.32:7077") \
    .config("spark.driver.host", "192.168.0.32") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .getOrCreate()

df = spark.range(10)
df.show()

spark.stop()
