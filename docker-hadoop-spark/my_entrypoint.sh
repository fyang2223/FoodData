#!/bin/bash

/bin/cp "/AirflowAuth/AirflowKey.pub" "/home/sshuser/.ssh/authorized_keys"
chown sshuser:sshgroup /home/sshuser/.ssh/authorized_keys && chmod 700 /home/sshuser/.ssh/authorized_keys
service ssh start

#Test: from airflow worker's /opt/airflow/keys directory, ssh to hadoop namenode using:
# ssh -i AirflowKey sshuser@namenode -p 22
# OR, if from the /opt/airflow ($AIRFLOW_HOME) directory,
# ssh -i keys/AirflowKey sshuser@namenode -p 22