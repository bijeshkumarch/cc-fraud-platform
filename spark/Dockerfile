FROM eclipse-temurin:17-jdk

USER root

# Install Python and other system deps
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    curl \
    procps \
  && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/local/openjdk-17

# Install Spark binaries
ENV SPARK_VERSION=3.5.2
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    -o /tmp/spark.tgz \
 && mkdir -p /opt && tar -xzf /tmp/spark.tgz -C /opt \
 && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} \
 && rm /tmp/spark.tgz

ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

# Python dependencies used by streaming and jobs
RUN python3 -m pip install --break-system-packages --no-cache-dir \
    joblib \
    scikit-learn \
    pandas \
    numpy \
    pyarrow \
    pyspark

USER 1001
