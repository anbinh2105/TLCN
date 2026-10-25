
FROM tlcn-spark:4.1.1 AS spark-client

FROM apache/airflow:3.1.5

USER root

# Reuse Java 21 and Spark 4.1.1 from the tested Spark image.
COPY --from=spark-client /opt/java/openjdk /opt/java/openjdk
COPY --from=spark-client /opt/spark /opt/spark

ENV JAVA_HOME=/opt/java/openjdk
ENV SPARK_HOME=/opt/spark
ENV PATH="${JAVA_HOME}/bin:${SPARK_HOME}/bin:${PATH}"
ENV PYSPARK_PYTHON=python
ENV PYSPARK_DRIVER_PYTHON=python
ENV PYTHONPATH="/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip"

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow-providers-apache-spark==6.3.3" \
    "python-dotenv==1.2.1" \
    "minio" \
    "openpyxl" \
    "dbt-core==1.10.15" \
    "dbt-trino==1.10.1"
