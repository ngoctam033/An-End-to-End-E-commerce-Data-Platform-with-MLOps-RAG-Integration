FROM tabulario/spark-iceberg
USER root

# Cài đặt Jupyter từ local python packages (setup/) để tránh tải lại
COPY setup/python_packages /tmp/python_packages
RUN pip install --no-cache-dir --no-index --find-links=/tmp/python_packages jupyterlab notebook && \
    mkdir -p /home/iceberg/notebooks

# Copy local JARs
COPY jars/hadoop-aws-3.3.4.jar /opt/spark/jars/
COPY jars/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/
COPY jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar /opt/spark/jars/
COPY jars/postgresql-42.6.0.jar /opt/spark/jars/