FROM apache/airflow:3.1.6

USER root

# CẤU HÌNH PHIÊN BẢN SPARK
# QUAN TRỌNG: Phải khớp hoàn toàn với phiên bản trong container spark-worker
# Nếu spark-worker dùng 3.5.5, hãy đổi thành 3.5.5
ENV SPARK_VERSION=3.5.5
ENV HADOOP_VERSION=3

# Cài đặt Java và Spark binaries từ file local (setup/)
# Thêm procps để khắc phục lỗi "ps: command not found"
COPY setup/OpenJDK17U-jdk_x64_linux_hotspot_17.0.10_7.tar.gz /tmp/
COPY setup/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz /tmp/

RUN apt-get update \
    && apt-get install -y --no-install-recommends procps \
    && mkdir -p /usr/lib/jvm/java-17-openjdk-amd64 \
    && tar -xzf /tmp/OpenJDK17U-jdk_x64_linux_hotspot_17.0.10_7.tar.gz -C /usr/lib/jvm/java-17-openjdk-amd64 --strip-components=1 \
    && tar -xzf /tmp/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark \
    && rm /tmp/OpenJDK17U-jdk_x64_linux_hotspot_17.0.10_7.tar.gz \
    && rm /tmp/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy local JARs instead of downloading them
COPY jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar /opt/spark/jars/
COPY jars/hadoop-aws-3.3.4.jar /opt/spark/jars/ 
COPY jars/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/
COPY jars/postgresql-42.6.0.jar /opt/spark/jars/

ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Copy requirements và local packages (nếu có)
COPY --chown=airflow:0 airflow/requirements.txt /tmp/requirements.txt
COPY --chown=airflow:0 setup/python_packages /tmp/python_packages

# Bắt buộc cài đặt offline (--no-index) từ thư mục /tmp/python_packages
# Điều này đảm bảo hoàn toàn không tải lại từ internet mỗi lần build
RUN pip install --no-cache-dir --no-index --find-links=/tmp/python_packages -r /tmp/requirements.txt