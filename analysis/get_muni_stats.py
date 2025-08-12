from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MUNIExplore").getOrCreate()

# Start with just one file to avoid the network issue
try:
    df = spark.read.parquet("/mnt/ssd/raw/vehicle/vehicles_20250807_114223.parquet")
    
    print("=== SCHEMA ===")
    df.printSchema()
    
    print(f"\n=== ROW COUNT ===")
    print(f"Rows: {df.count()}")
    
    print("\n=== SAMPLE DATA ===")
    df.show(10, truncate=False)
    
except Exception as e:
    print(f"Error reading single file: {e}")
    
spark.stop()