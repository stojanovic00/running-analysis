#!/bin/bash

#$1 - name of topic to delete
if [ -z "$1" ]; then
    echo "Usage: $0 <topic_name>"
    exit 1
fi

docker exec kafka1 kafka-topics --delete --topic $1 --bootstrap-server "kafka1:19092,kafka2:19093,kafka3:19094"

