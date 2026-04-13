FROM apache/spark:3.5.3

USER root

# Install envsubst for config template substitution
RUN apt-get update && apt-get install -y --no-install-recommends gettext-base \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Python packages for Delta Lake + ML
RUN pip install --no-cache-dir \
    delta-spark==3.1.0 \
    pyspark==3.5.3 \
    mlflow==2.12.1 \
    scikit-learn==1.3.2 \
    pandas==2.0.3 \
    numpy==1.24.4 \
    boto3==1.34.0 \
    joblib==1.3.2 \
    redis==5.0.1 \
    great_expectations \
    joblib==1.3.2

# Download required JARs for Delta Lake + S3/MinIO support
RUN curl -L -o /opt/spark/jars/delta-spark_2.12-3.1.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar && \
    curl -L -o /opt/spark/jars/delta-storage-3.1.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar && \
    curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -L -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.3.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar && \
    curl -L -o /opt/spark/jars/kafka-clients-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar && \
    curl -L -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar && \
    curl -L -o /opt/spark/jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# Copy config template and entrypoint
COPY spark-defaults.conf.template /opt/spark/conf/spark-defaults.conf.template
COPY entrypoint.sh /opt/spark/entrypoint.sh
RUN chmod +x /opt/spark/entrypoint.sh && \
    chown spark:spark /opt/spark/conf/spark-defaults.conf.template && \
    chown spark:spark /opt/spark/conf/spark-defaults.conf || true && \
    chmod 775 /opt/spark/conf/

ENV SPARK_CONF_DIR=/tmp/spark-conf

USER spark

ENTRYPOINT ["/opt/spark/entrypoint.sh"]
