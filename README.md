# Overview
This project pulls data from the publicly available datasets on the USDA website (https://fdc.nal.usda.gov/download-datasets.html#bkmk-2). The full collection is downloaded (under subheading "Full Download of All Data Types"), which is a 1.3GB collection of CSVs updated every 6 months of food labelling and nutritional information. The output returned after processing is a single CSV dataset which consists of the average nutritional amounts for each food category as defined by the USDA. This output is small enough to be consumed by any in-memory data analysis tool for visualisation or dashboarding.

# Architecture Components
![Container Architecture](/assets/container_architecture.png)

# Directory Structure
The ./airflow/data folder on the host machine is where the data is downloaded and unzipped to, and is mounted to both Airflow (/opt/airflow/data) as well as the Hadoop namenode (/data). This is so that the downloaded files can be put into the HDFS filesystem with the simple command `hdfs dfs -put` in the namenode after being downloaded and extracted.

The "KEYS" folder is empty for security but should contain an SSH keypair so that Airflow can use the SSHOperator to connect to the Hadoop namenode. These keys need to be generated externally using 
```
ssh-keygen -t ed25519 -N "" -f "AirflowKey"
```
where `-t` is the type, `-N` is to specify a passphrase that is an empty string, and `-f` is to specify the output filename. The "KEYS" folder is also mounted to both Airflow (/opt/airflow/keys) and the Hadoop namenode (though only the public key is necessary in the namenode as it is copied to the /root/.ssh/authorized_keys file).

The `my_entrypoint.sh` and `load_data.sh` files in the docker-hadoop-spark directory are necessary to execute commands on the namenode.

The default docker-compose file of the BDE2020 Hadoop-Spark-Hive project has been modified to only run Hadoop, and to build the namenode from a dockerfile, which installs OpenSSH and copies some shell scripts to be used once the container is running.

The default docker-compose file of Airflow has also been modified to install the Java and Spark binaries.

# Installation Instructions
1. Start the Airflow docker-compose file first in the airflow folder, as the airflow_default network needs to be created before Spark and HDFS can initialize.
```
docker-compose build
docker-compose up airflow-init
docker-compose up
```

2. Enter the `Spark` folder, and start the Spark cluster with docker-compose.
```
docker-compose build
docker-compose up
```

3. Enter the `docker-hadoop-spark` folder and start the Hadoop cluster with docker-compose. If `docker-compose build` fails, delete the namenode image in Docker Desktop first, and try again.

```
docker-compose build
docker-compose up
```

4. Enter the running namenode container with docker exec, and execute the `my_entrypoint.sh` script, which copies Airflow's public key located in the mounted volume to the authorized_keys file in the namenode root user's home volume (which is /root, but is /home/user1 for another user called user1). It then starts the OpenSSH service.
```
docker exec -it namenode bash
```
```
./entrypoint.sh
```

5. Set up the Spark and HDFS connections in Airflow as follows so that there can be communication between the Airflow and Spark/HDFS containers.

![Spark Connection](/assets/spark_connection.png)

![HDFS Connection](/assets/hdfs_connection.png)

# Running the ETL Pipeline
1. After setup of the architecture, visit `localhost:8080` in a browser to run the "USDA_ETL_DAG" DAG.

2. For proof of concept, the DAG has been set to run once a day from 25/04/2022 to 30/04/2022, so a simple change to the `end_date` parameter in `data_ingest.py` can allow the DAG to run into the future.

3. Only the 28/04/2022 job will succeed, as that is when the most recent USDA food data was uploaded.

4. The output is located in `/airflow/data/output.csv`.

5. Matplotlib has been used to create a graph to visualise an excerpt of the `output.csv` data. This file is called `CarbohydrateContent.png` and can be found in the home folder.

![Visualisation](/assets/CarbohydrateContent.png)

![HDFS Connection](/assets/DAGrun.png)

# LINKS
- https://medium.com/codex/executing-spark-jobs-with-apache-airflow-3596717bbbe3