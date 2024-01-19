#!/bin/bash

##$1 - name of topic 
if [ -z "$1" ]; then
    echo "Usage: $0 <topic_name>"
    exit 1
fi

docker exec -it  kafka1 kafka-console-consumer \
  --bootstrap-server "kafka1:19092,kafka2:19093,kafka3:19094" \
  --topic $1 \



# Prints parsed timestamp if needed
 #--property print.timestamp=true \
#| awk '{ match($1, /CreateTime:([0-9]+)/, arr); printf "timestamp: %s  value: %s\n", strftime("%Y-%m-%d %H:%M:%S", arr[1] / 1000), $2; fflush(); }'

