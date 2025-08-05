from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MacDriverTest") \
    .master("spark://192.168.0.143:7077") \
    .config("spark.executor.memory", "512m") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()

df = spark.range(1000000).toDF("number")
print("Row count:", df.count())

spark.stop()
