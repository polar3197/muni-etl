from pyspark.sql import SparkSession

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkConf
import os


jar_path ="/Users/charliecooper/Code/packages/aws-hadoop/aws-java-sdk-bundle-1.12.262.jar,/Users/charliecooper/Code/packages/aws-hadoop/hadoop-aws-3.3.2.jar"

AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
if AWS_KEY is None:
    raise ValueError("AWS_SECRET_ACCESS_KEY not set")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
if AWS_SECRET_KEY is None:
    raise ValueError("AWS_SECRET_ACCESS_KEY not set")

spark = SparkSession.builder \
    .appName("Query Parquet in S3") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config("spark.hadoop.fs.s3a.access.key", AWS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.fast.upload", "true") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.executor.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true") \
    .config("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true") \
    .getOrCreate()  # Use getOrCreate() to prevent multiple Spark contexts

df = spark.range(100)
df.show()
# df = spark.read.parquet("s3a://muni-parquet-data/year=2025/month=7/day=18/")
# df.show()
