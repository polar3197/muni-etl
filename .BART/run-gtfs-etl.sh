#!/bin/bash

# GTFS Protocol Buffer to PostgreSQL ETL Runner
# This script runs the GTFS ETL process on the Spark cluster

set -e

# Configuration
SPARK_MASTER="spark://192.168.0.32:7077"
PYSPARK_SCRIPT="gtfs_spark_etl.py"
JARS_DIR="./jars"

# Create jars directory if it doesn't exist
mkdir -p $JARS_DIR

# Download PostgreSQL JDBC driver if not present
if [ ! -f "$JARS_DIR/postgresql-42.7.1.jar" ]; then
    echo "Downloading PostgreSQL JDBC driver..."
    curl -L -o $JARS_DIR/postgresql-42.7.1.jar \
        https://jdbc.postgresql.org/download/postgresql-42.7.1.jar
fi

# Set environment variables
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
export SPARK_HOME="/opt/spark"

# Run the PySpark job
echo "Starting GTFS ETL job on Spark cluster..."
echo "Master: $SPARK_MASTER"
echo "Script: $PYSPARK_SCRIPT"

spark-submit \
    --master $SPARK_MASTER \
    --deploy-mode cluster \
    --driver-memory 1g \
    --executor-memory 1g \
    --executor-cores 1 \
    --jars $JARS_DIR/postgresql-42.7.1.jar \
    --packages org.postgresql:postgresql:42.7.1 \
    --conf "spark.sql.adaptive.enabled=true" \
    --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
    --conf "spark.sql.adaptive.skewJoin.enabled=true" \
    --conf "spark.driver.extraJavaOptions=-Dfile.encoding=UTF-8" \
    --conf "spark.executor.extraJavaOptions=-Dfile.encoding=UTF-8" \
    $PYSPARK_SCRIPT

echo "GTFS ETL job completed!" 