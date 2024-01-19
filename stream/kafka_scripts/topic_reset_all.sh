#!/bin/bash

./topic_delete.sh run_stats
./topic_delete.sh speed
./topic_delete.sh avg_speed
./topic_delete.sh current_speed_comp

./topic_create.sh run_stats
./topic_create.sh speed
./topic_create.sh avg_speed
./topic_create.sh current_speed_comp
