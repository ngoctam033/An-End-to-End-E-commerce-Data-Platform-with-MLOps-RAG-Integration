FROM tabulario/spark-iceberg

USER root

# Tải các JARs cần thiết cho S3A (Hadoop 3.3.4 phù hợp với Spark 3.5)
# Chúng ta tải vào /opt/spark/jars vì biến IJAVA_CLASSPATH của bạn đang trỏ vào đó
RUN curl -s https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -o /opt/spark/jars/hadoop-aws-3.3.4.jar && \
    curl -s https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

# Chỉnh lại quyền sở hữu cho user iceberg
RUN chown iceberg:iceberg /opt/spark/jars/hadoop-aws-3.3.4.jar /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

USER iceberg