FROM python:3.11-slim

USER root

# Java nécessaire à Spark
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Installer Spark et ses dépendances Python
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
    pyspark==3.5.3 \
    numpy==2.2.6 \
    pandas==2.2.3 \
    scikit-learn==1.7.2 \
    joblib==1.6.0 \
    onnxruntime==1.31.1 \
    pyarrow==18.1.0 \
    cassandra-driver

ENV PYTHONUNBUFFERED=1
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3