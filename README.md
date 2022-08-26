# NOTES
- Spark uses the network airflow_default so launch airflow FIRST.
- The Dockerfile has been updated to execute "USER airflow" and "RUN pip install apache-airflow-providers-apache-spark"
- SparkSubmiteOperator uses the airflow directory to read from AND output to (once execution of the pyspark script has finished).
- Add a connection in the airflow UI. "Conn_Id" can be any unique name. Host is spark://"spark", since "spark" is the name of the service in the bitnami/spark docker-compose file, and they are running in the same network (airflow_default).
- Connections in airflow:
    spark_default: "spark" conn type, "spark://spark" host, "7077" port.
    hadoop_default: "ssh" conn type, "namenode" host, "sshuser" username, "22" port, "{"key_file": "/opt/airflow/keys/AirflowKey"}" extra.
    
# LINKS
- https://medium.com/codex/executing-spark-jobs-with-apache-airflow-3596717bbbe3