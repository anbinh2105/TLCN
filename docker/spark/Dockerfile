
FROM python:3.12-slim-bookworm AS python-runtime

FROM apache/spark:4.1.1

USER root

WORKDIR /opt/project

# Install Python 3.12 runtime and its standard library.
COPY --from=python-runtime /usr/local /usr/local

ENV PATH="/usr/local/bin:${PATH}"
ENV PYSPARK_PYTHON=/usr/local/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3

COPY docker/spark/requirements.txt /tmp/spark-requirements.txt

RUN python3 -m pip install --no-cache-dir \
    -r /tmp/spark-requirements.txt

# Download Iceberg and Hadoop S3A dependencies.
RUN mkdir -p /tmp/spark-deps && \
    /opt/spark/bin/spark-submit \
      --master "local[1]" \
      --packages \
      org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0,org.apache.hadoop:hadoop-aws:3.4.2 \
      /opt/spark/examples/src/main/python/pi.py 1 && \
    cp /root/.ivy2.5.2/jars/*.jar /opt/spark/jars/

USER spark
