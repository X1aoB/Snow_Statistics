ARG HADOOP_IMAGE
ARG SPARK_IMAGE
FROM ${HADOOP_IMAGE} AS hadoop
FROM ${SPARK_IMAGE}
USER root
COPY --from=hadoop /opt/hadoop /opt/hadoop
ENV HADOOP_HOME=/opt/hadoop HADOOP_COMMON_HOME=/opt/hadoop HADOOP_HDFS_HOME=/opt/hadoop HADOOP_YARN_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/etc/hadoop JAVA_HOME=/opt/java/openjdk
ENV PATH=/opt/hadoop/bin:/opt/hadoop/sbin:${PATH}
ENTRYPOINT ["/opt/hadoop/bin/yarn"]
