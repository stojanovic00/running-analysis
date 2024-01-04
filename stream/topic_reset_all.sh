#!/bin/bash

./topic_delete.sh run_stats
./topic_delete.sh speed
./topic_delete.sh avg-speed
./topic_delete.sh heart_rate

./topic_create.sh run_stats
./topic_create.sh speed
./topic_create.sh avg-speed
./topic_create.sh heart_rate


