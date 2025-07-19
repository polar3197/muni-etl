#!/bin/bash

# Ensure Java 11 is used for this script only
export JAVA_HOME=$(/usr/libexec/java_home -v 11)

# Point to your Spark install
export SPARK_HOME="$HOME/Code/packages/spark-3.5.6-bin-hadoop3"
export PATH="$SPARK_HOME/bin:$PATH"

# Optional: Set your Spark IP explicitly if you want to suppress loopback warnings
export SPARK_LOCAL_IP=192.168.0.32

# Launch PySpark using your remote master
pyspark --master=spark://192.168.0.32:7077 

