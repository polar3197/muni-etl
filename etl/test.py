# from pyspark.sql import SparkSession


from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkConf
import os

AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
if AWS_KEY is None:
    raise ValueError("AWS_ACCESS_KEY not set")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
if AWS_SECRET_KEY is None:
    raise ValueError("AWS_SECRET_ACCESS_KEY not set")

spark = SparkSession.builder \
    .appName("Query Parquet in S3") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.11.1026") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# df = spark.range(100)
# df.show()
df = spark.read.parquet("s3a://muni-parquet-data/")
df.createOrReplaceTempView("vehicles")
spark.sql("""
    SELECT route_id, year, month, day, hour, minute, vehicle_id, occupancy_status
    FROM vehicles
    WHERE route_id = 'N'
""").show(100)

df_filtered = df.filter(
    (df.route_id == "N") &
    (df.year == 2025) &
    (df.month == 7) &
    (df.day == 18) &
    (df.hour == 15) &
    (df.minute == 40)
)


print(df_filtered.select("vehicle_id").distinct().count())

# df_38r = df.filter(df.route_id == "38R")
# df_38r.show()
#df.show()
