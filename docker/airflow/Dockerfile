FROM apache/spark:4.1.3-scala2.13-java17-python3-ubuntu AS spark-jars
USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/* \
    && curl -fsSL -o /opt/spark/jars/iceberg-spark-runtime-4.1_2.13-1.11.0.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.11.0/iceberg-spark-runtime-4.1_2.13-1.11.0.jar \
    && curl -fsSL -o /opt/spark/jars/iceberg-nessie-1.11.0.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-nessie/1.11.0/iceberg-nessie-1.11.0.jar \
    && curl -fsSL -o /opt/spark/jars/hadoop-aws-3.4.2.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.2/hadoop-aws-3.4.2.jar \
    && curl -fsSL -o /opt/spark/jars/aws-sdk-bundle-2.29.52.jar https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar \
    && curl -fsSL -o /opt/spark/jars/aws-sdk-auth-2.29.52.jar https://repo1.maven.org/maven2/software/amazon/awssdk/auth/2.29.52/auth-2.29.52.jar

FROM apache/airflow:2.10.5-python3.11
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jdk-headless && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"
USER airflow
COPY requirements/airflow.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --disable-pip-version-check -r /tmp/requirements.txt
# Airflow launches spark-submit locally. Ship the same connectors used by Spark
# so DAG tasks never invoke Maven while the platform is running offline.
COPY --from=spark-jars /opt/spark/jars/ /home/airflow/.local/lib/python3.11/site-packages/pyspark/jars/
