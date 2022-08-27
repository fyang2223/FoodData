# Overview
- Spark uses the network airflow_default so launch airflow FIRST.
- The Dockerfile has been updated to execute "USER airflow" and "RUN pip install apache-airflow-providers-apache-spark"
- SparkSubmiteOperator uses the airflow directory to read from AND output to (once execution of the pyspark script has finished).
- Add a connection in the airflow UI. "Conn_Id" can be any unique name. Host is spark://"spark", since "spark" is the name of the service in the bitnami/spark docker-compose file, and they are running in the same network (airflow_default).
- Connections in airflow:
    spark_default: "spark" conn type, "spark://spark" host, "7077" port.
    hadoop_default: "ssh" conn type, "namenode" host, "root" username, "22" port, "{"key_file": "/opt/airflow/keys/AirflowKey"}" extra.

# Architecture
This project uses docker-compose files.

The ./airflow/data folder on the host machine is where the data is downloaded and unzipped to, and is mounted to both Airflow (/opt/airflow/data) as well as the Hadoop namenode (/data). This is so that the downloaded files can be put into the HDFS filesystem with the simple command `hdfs dfs -put` in the namenode after being downloaded and extracted.

The "KEYS" folder is not included in the top-level github directory but should contain an SSH keypair so that Airflow can use the SSHOperator to connect to the Hadoop namenode. These keys need to be generated externally using 
```
ssh-keygen -t ed25519 -N "" -f "AirflowKey"
```
where `-t` is the type, `-N` is to specify a passphrase that is an empty string, and `-f` is to specify the output filename. The "KEYS" folder is also mounted to both Airflow (/opt/airflow/keys) and the Hadoop namenode (though only the public key is necessary in the namenode as it is copied to the /root/.ssh/authorized_keys file).

The `my_entrypoint.sh` and `load_data.sh` files in the docker-hadoop-spark directory are necessary to execute commands on the namenode.

The default docker-compose file of the BDE2020 Hadoop-Spark-Hive project has been modified to only run Hadoop, and to build the namenode from a dockerfile, which installs OpenSSH and copies some shell scripts to be used once the container is running.

The default docker-compose file of Airflow has also been modified to install the Java and Spark binaries.

# Installation Instructions
1. Start the Airflow docker-compose file first in the airflow folder.
```
docker-compose build
docker-compose up airflow-init
docker-compose up
```

2. Start the Hadoop cluster with docker-compose in the docker-hadoop-spark folder. 
```
docker-compose build
docker-compose up
```
If `docker-compose build` fails, delete the namenode image in Docker Desktop first, and try again.

3. Enter the running namenode container, and execute the `my_entrypoint.sh` script, which copies Airflow's public key located in the mounted volume to the authorized_keys file in the namenode root user's home volume (which is /root, but is /home/user1 for another user called user1). It then starts the SSH service.
```
docker exec -it namenode bash
```
```
./entrypoint.sh
```

4. 

x. Set up connections in airflow
conn id: spark_default
conn type: spark
host: spark://spark
port: 7077

conn id: hadoop_default
conn type: ssh
host: namenode
username: root
port: 22
extra: {"key_file": "/opt/airflow/keys/AirflowKey"}



# LINKS
- https://medium.com/codex/executing-spark-jobs-with-apache-airflow-3596717bbbe3