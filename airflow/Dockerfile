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
#RUN pip install pyspark

# Create ssh keypair on airflow machine. -q quiet, -t keytype, -N passphrase, -C unique name, -f outputfile
# RUN ssh-keygen -q -t ed25519 -N "" -C "AirflowKey" -f /opt/airflow/AirflowKey <<< y
# DONT DO ABOVE. Instead, the mounted keys volume should have user-generated public and private keys in them already with the filenames "AirflowKey(.pub)"
# ssh-keygen -t ed25519 -N "" -C "AirflowKey" -f ~/Documents/Docker_Project/airflow/keys/AirflowKey

USER airflow

RUN pip install apache-airflow-providers-apache-spark

USER root