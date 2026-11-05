FROM apache/spark:3.5.3

USER root

# Installation des dépendances Python nécessaires pour l'inférence ONNX et Cassandra
RUN pip install --no-cache-dir \
    onnxruntime \
    pyarrow \
    pandas \
    numpy \
    cassandra-driver

USER 1001