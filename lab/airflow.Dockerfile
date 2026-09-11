ARG SPARK_IMAGE
ARG AIRFLOW_IMAGE
FROM ${SPARK_IMAGE} AS spark
FROM ${AIRFLOW_IMAGE}
COPY --from=spark /opt/spark /opt/spark
COPY --from=spark /opt/java/openjdk /opt/java/openjdk
ENV SPARK_HOME=/opt/spark JAVA_HOME=/opt/java/openjdk HADOOP_CONF_DIR=/etc/hadoop
ENV PATH=/opt/spark/bin:/opt/java/openjdk/bin:${PATH}
