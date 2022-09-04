FROM apache/airflow:2.3.3

USER root

RUN apt-get update -qq
RUN apt-get install unzip

# Install OpenJDK-11 on Airflow container
RUN apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME on Airflow container
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
RUN export JAVA_HOME

# Install the Spark binary
RUN curl https://archive.apache.org/dist/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz -o spark.tgz && tar -xf spark.tgz
RUN mv spark-3.3.0-bin-hadoop3 /opt

# Set SPARK_HOME
ENV SPARK_HOME /opt/spark-3.3.0-bin-hadoop3
RUN export SPARK_HOME
RUN chmod 777 $SPARK_HOME

#ENV PATH $PATH:/opt/spark-3.3.0-bin-hadoop3/bin
RUN export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

USER airflow

RUN pip install apache-airflow-providers-apache-spark pandas matplotlib

USER root