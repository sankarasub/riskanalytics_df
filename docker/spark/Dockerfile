FROM apache/spark:4.1.3-scala2.13-java17-python3-ubuntu
USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
# Store connectors in Spark's standard jars directory. Spark loads this directory
# automatically, eliminating runtime --packages/Ivy resolution.
COPY requirements/spark.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir --disable-pip-version-check -r /tmp/requirements.txt
RUN mkdir -p /opt/spark/.ivy2 /opt/spark/jars && chown -R spark:spark /opt/spark/.ivy2 /opt/spark/jars
RUN curl -fsSL -o /opt/spark/jars/iceberg-spark-runtime-4.1_2.13-1.11.0.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.11.0/iceberg-spark-runtime-4.1_2.13-1.11.0.jar \
    && curl -fsSL -o /opt/spark/jars/iceberg-nessie-1.11.0.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-nessie/1.11.0/iceberg-nessie-1.11.0.jar \
    && curl -fsSL -o /opt/spark/jars/hadoop-aws-3.4.2.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar \
    && curl -fsSL -o /opt/spark/jars/aws-sdk-bundle-2.29.52.jar https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar \
    && curl -fsSL -o /opt/spark/jars/aws-sdk-auth-2.29.52.jar https://repo1.maven.org/maven2/software/amazon/awssdk/auth/2.29.52/auth-2.29.52.jar \
    && curl -fsSL -o /opt/spark/jars/spark-connect_2.13-4.1.3.jar https://repo1.maven.org/maven2/org/apache/spark/spark-connect_2.13/4.1.3/spark-connect_2.13-4.1.3.jar \
    && curl -fsSL -o /opt/spark/jars/spark-sql-kafka-0-10_2.13-4.1.3.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.1.3/spark-sql-kafka-0-10_2.13-4.1.3.jar \
    && curl -fsSL -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.13-4.1.3.jar https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.1.3/spark-token-provider-kafka-0-10_2.13-4.1.3.jar \
    && curl -fsSL -o /opt/spark/jars/kafka-clients-3.9.1.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar
ENV HOME=/opt/spark
ENV SPARK_HOME=/opt/spark
ENV PATH=/opt/spark/bin:$PATH
USER spark
