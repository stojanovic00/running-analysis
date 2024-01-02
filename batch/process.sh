#!/bin/bash

docker exec spark-master ./spark/bin/spark-submit --jars hdfs://namenode:9000/user/aleksandar/libs/postgresql-42.7.1.jar \
                                                  ./batch-scripts/process.py