#!/bin/bash

docker exec namenode hdfs dfs -copyFromLocal /scripts/libs/postgresql-42.7.1.jar /user/aleksandar/libs
