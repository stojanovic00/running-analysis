#!/bin/bash

##$1 - name of topic to create
if [ -z "$1" ]; then
    echo "Usage: $0 <topic_name>"
    exit 1
fi


docker exec kafka1 kafka-topics --create --topic $1 --bootstrap-server kafka1:9092 --partitions 1 --replication-factor 3


