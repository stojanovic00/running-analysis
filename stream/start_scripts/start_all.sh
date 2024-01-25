#!/bin/bash

./speed_stream.sh &
sleep 30

./pace_stream.sh &
sleep 30

./avg_speed_stream.sh &
sleep 30

./current_speed_comp_stream.sh &
sleep 30

./altitude_gains_stream.sh &
sleep 30

./elevation_gain_stream.sh &
sleep 30

./finish_estimation_stream.sh &
