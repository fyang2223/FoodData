#!/bin/bash

## THESE HAVE TO BE DONE MANUALLY THROUGH DOCKER EXEC -IT NAMENODE
/bin/cp "/AirflowAuth/AirflowKey.pub" "/root/.ssh/authorized_keys"
chown root:root /root/.ssh/authorized_keys && chmod 700 /root/.ssh/authorized_keys
service ssh start

#Test: from airflow worker's /opt/airflow directory, ssh to hadoop namenode using:
# ssh -i keys/AirflowKey sshuser@namenode -p 22