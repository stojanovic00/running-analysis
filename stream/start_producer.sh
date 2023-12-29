#!/bin/bash
#
docker exec -it  kafka1 kafka-console-producer --broker-list kafka1:9092 --topic RUN_STATS

