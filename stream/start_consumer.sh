#!/bin/bash

##$1 - name of topic 
if [ -z "$1" ]; then
    echo "Usage: $0 <topic_name>"
    exit 1
fi

docker exec -it  kafka1 kafka-console-consumer --bootstrap-server "kafka1:19092,kafka2:19093,kafka3:19094"  --topic $1

# this is for proper double deser
#--property value.deserializer=org.apache.kafka.common.serialization.DoubleDeserializer
