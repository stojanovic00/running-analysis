docker exec spark-master ./spark/bin/spark-submit \
                          --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 \
                          ./stream-scripts/avg_speed_stream.py
