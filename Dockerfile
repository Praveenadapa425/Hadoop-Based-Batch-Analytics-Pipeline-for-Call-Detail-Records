FROM apache/airflow:2.8.0 AS runtime

USER root

ARG SPARK_VERSION=3.5.8
ARG HADOOP_VERSION=3

ENV DEBIAN_FRONTEND=noninteractive
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV PYTHONPATH=/opt/cdr

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash curl ca-certificates openjdk-17-jre-headless tar wget \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/spark \
    && wget -O /tmp/spark.tgz \
       "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && tar -xzf /tmp/spark.tgz -C /opt/spark --strip-components=1 \
    && rm /tmp/spark.tgz

# COPY spark-3.5.8-bin-hadoop3.tgz /tmp/

# RUN mkdir -p /opt/spark \
#     && tar -xzf /tmp/spark-3.5.8-bin-hadoop3.tgz \
#        -C /opt/spark --strip-components=1
      
      
WORKDIR /opt/cdr

COPY data /opt/cdr/data
COPY jobs /opt/cdr/jobs
COPY src /opt/cdr/src
COPY dags /opt/airflow/dags
COPY run_pipeline.sh /opt/cdr/run_pipeline.sh

RUN chmod +x /opt/cdr/data/generate_records.sh /opt/cdr/run_pipeline.sh

USER airflow
