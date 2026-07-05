FROM apache/airflow:2.8.1-python3.10

USER root

# System dependencies (needed for Spark control, signals, etc.)
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    procps \
    curl \
    && apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Install Spark (binaries) so `spark-submit` is available in the Airflow image.
# We install as root then drop back to the `airflow` user.
ENV SPARK_VERSION=3.5.2
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    -o /tmp/spark.tgz \
    && mkdir -p /opt && tar -xzf /tmp/spark.tgz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} \
    && rm /tmp/spark.tgz

ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"
ENV PYTHONPATH=/opt/project:/opt/airflow

# Install Python dependencies as root so the `airflow` CLI is available system-wide
COPY requirements.txt /requirements.txt

# Install Python dependencies as the `airflow` user (recommended by upstream)
USER airflow
ENV PATH="/home/airflow/.local/bin:${PATH}"
RUN pip install --user --no-cache-dir -r /requirements.txt

# Copy DAGs
USER root
COPY airflow/dags /opt/airflow/dags

# Copy project code Airflow needs
COPY batch /opt/airflow/batch
COPY common /opt/airflow/common

# Ensure airflow CLI is available system-wide and fix ownership
RUN ln -sf /home/airflow/.local/bin/airflow /usr/local/bin/airflow || true \
    && chown -R airflow: /opt/airflow /opt/airflow/dags /opt/airflow/plugins /opt/airflow/logs /opt/airflow/batch /opt/airflow/common || true

USER airflow
WORKDIR /opt/airflow
