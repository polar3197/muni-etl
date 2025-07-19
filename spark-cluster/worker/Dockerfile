FROM --platform=linux/arm64 python:3.10-slim

# Install dependencies, download & extract Spark, cleanup
ENV SPARK_VERSION="3.5.6"
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-arm64"
ENV SPARK_HOME="/opt/spark"
ENV PYSPARK_PYTHON="python3"
ENV PATH="${JAVA_HOME}/bin:${SPARK_HOME}/sbin:${SPARK_HOME}/bin:$PATH"

EXPOSE 7077 8080

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        wget \
        ca-certificates \
        gnupg \
        procps \
        iproute2 && \
    rm -rf /var/lib/apt/lists/* && \
    curl -L -o /tmp/spark.tgz https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME} && \
    rm /tmp/spark.tgz && \
    rm -rf ${SPARK_HOME}/examples \
           ${SPARK_HOME}/jars/*mesos* \
           ${SPARK_HOME}/jars/*kubernetes* \
           ${SPARK_HOME}/jars/*hive* \
           ${SPARK_HOME}/jars/*avro* \
           ${SPARK_HOME}/jars/*parquet* \
           ${SPARK_HOME}/jars/*orc*

# Install PySpark
RUN pip install --no-cache-dir pyspark

# Configuration scripts
COPY conf/spark-env.sh ${SPARK_HOME}/conf/spark-env.sh
COPY worker/start-worker.sh /opt/start-worker.sh

RUN chmod +x ${SPARK_HOME}/conf/spark-env.sh /opt/start-worker.sh

CMD ["/opt/start-worker.sh"]