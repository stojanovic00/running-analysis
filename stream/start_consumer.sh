#!/bin/bash

docker exec -it  kafka1 kafka-console-consumer --bootstrap-server "kafka1:19092,kafka2:19093,kafka3:19094"  --topic run_stats
 # --from-beginning can be added
