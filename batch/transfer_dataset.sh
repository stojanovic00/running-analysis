#!/bin/bash

#Copy batch dataset to hdfs
docker exec namenode hdfs dfs -copyFromLocal /localDatasets/batch_sample.csv /user/aleksandar/data
docker exec namenode hdfs dfs -copyFromLocal /localDatasets/ultramarathons.csv /user/aleksandar/data
