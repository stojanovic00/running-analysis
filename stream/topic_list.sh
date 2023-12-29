#!/bin/bash

docker exec kafka1 kafka-topics --list --bootstrap-server localhost:9092
