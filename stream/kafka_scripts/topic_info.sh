#!/bin/bash

##$1 - name of topic
if [ -z "$1" ]; then
    echo "Usage: $0 <topic_name>"
    exit 1
fi


docker exec kafka1 kafka-topics --describe --topic $1 --zookeeper zoo:2181



