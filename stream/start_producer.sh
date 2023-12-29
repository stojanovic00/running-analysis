#!/bin/bash
#
docker exec -it  kafka1 kafka-console-producer --broker-list "kafka1:19092,kafka2:19093,kafka3:19094"  --topic RUN_STATS

