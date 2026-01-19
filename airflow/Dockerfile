FROM apache/airflow:3.1.6

USER root

# Cài đặt Java (OpenJDK 11 hoặc 17 thường dùng cho Spark)
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install pandas pyarrow \
                apache-airflow-providers-apache-spark \
                apache-airflow-providers-postgres \
                apache-airflow-providers-amazon

# Thiết lập biến môi trường JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow