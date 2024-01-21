docker exec spark-master ./spark/bin/spark-submit \
                          --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 \
                          --jars hdfs://namenode:9000/user/aleksandar/libs/postgresql-42.7.1.jar \
                          ./stream-scripts/current_speed_comp_stream.py
