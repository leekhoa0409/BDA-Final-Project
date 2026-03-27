FROM apache/airflow:2.7.3-python3.8

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jdk curl procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
