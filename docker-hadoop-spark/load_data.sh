#!/bin/bash

export PATH=/opt/hadoop-3.2.1/bin/:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

hdfs dfs -mkdir /data/FoodData
hdfs dfs -put /data/$1 /data/FoodData