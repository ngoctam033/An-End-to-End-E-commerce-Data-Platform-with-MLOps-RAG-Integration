FROM apache/airflow:3.1.6

USER root

# CẤU HÌNH PHIÊN BẢN SPARK
# QUAN TRỌNG: Phải khớp hoàn toàn với phiên bản trong container spark-worker
# Nếu spark-worker dùng 3.5.5, hãy đổi thành 3.5.5
ENV SPARK_VERSION=3.5.5
ENV HADOOP_VERSION=3

# Cài đặt Java và Spark binaries
# Thêm procps để khắc phục lỗi "ps: command not found"
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless wget procps \
    && wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Tải Iceberg Runtime và các thư viện Hadoop AWS
# Lưu ý: Các file jar này cũng phải tương thích với version Spark
# Dưới đây là cấu hình cho Spark 3.5.x (dùng iceberg-spark-runtime-3.5)
RUN curl -O https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar \
    && mv iceberg-spark-runtime-3.5_2.12-1.4.3.jar /opt/spark/jars/ \
    && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && mv hadoop-aws-3.3.4.jar /opt/spark/jars/ \
    && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    && mv aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/ \
    && curl -O https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar \
    && mv postgresql-42.6.0.jar /opt/spark/jars/

ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install pandas pyarrow \
                apache-airflow-providers-apache-spark \
                apache-airflow-providers-postgres \
                apache-airflow-providers-amazon