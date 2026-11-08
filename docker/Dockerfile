
FROM apache/airflow:3.1.7

USER root

# Install Java 17
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Add PostgreSQL JDBC driver
COPY docker/jars/postgresql-42.7.10.jar /usr/share/java/postgresql-42.7.10.jar

# Install pinned PySpark version
USER airflow

RUN python -m pip install --no-cache-dir "pyspark==4.3.0"
