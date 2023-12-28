#!/bin/bash

docker exec namenode hdfs dfs -mkdir -p /tmp
docker exec namenode hdfs dfs -mkdir -p /user/hive/warehouse
docker exec namenode hdfs dfs -chmod g+w /tmp
docker exec namenode hdfs dfs -chmod g+w /user/hive/warehouse
