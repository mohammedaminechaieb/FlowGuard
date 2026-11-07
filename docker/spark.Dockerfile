FROM python:3.11-slim-bookworm

# Java 17: the Java version Spark 3.5 officially supports
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java

ENV JAVA_HOME=/opt/java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Versions pinned to match the model artifacts:
#   scaler.pkl was saved with scikit-learn 1.9.1 and numpy 2.x
#   autoencoder.onnx is ONNX opset 18 (exported from PyTorch)
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
        pyspark==3.5.3 \
        numpy==2.2.6 \
        pandas==2.2.3 \
        pyarrow==18.1.0 \
        scikit-learn==1.9.1 \
        joblib==1.6.0 \
        onnxruntime==1.31.1 \
        cassandra-driver==3.29.2

# Download the Kafka and Cassandra connector JARs at build time,
# so the container doesn't need to fetch them on every start.
ENV SPARK_PACKAGES=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1
RUN python -c "from pyspark.sql import SparkSession; SparkSession.builder.master('local[1]').config('spark.jars.packages', '${SPARK_PACKAGES}').config('spark.jars.ivy', '/opt/ivy').getOrCreate().stop()"

ENV PYTHONUNBUFFERED=1
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

WORKDIR /opt/flowguard