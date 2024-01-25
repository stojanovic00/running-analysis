from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, window, avg, current_timestamp, split, to_timestamp, date_format, concat, \
    lit, expr

# Cluster config
conf = SparkConf() \
    .setAppName("avg-speed-streamer") \
    .setMaster("spark://spark-master:7077") \
    .set("spark.cores.max", "4") \
    .set("spark.executor.cores", "2")

spark = SparkSession.builder \
    .config(conf=conf) \
    .getOrCreate()


kafka_input_options = {
    "kafka.bootstrap.servers": "kafka1:19092,kafka2:19093,kafka3:19094",
    "subscribe": "speed"
}

# Read from Kafka using the Kafka source for Spark Structured Streaming
df = spark.readStream \
    .format("kafka") \
    .options(**kafka_input_options) \
    .option("failOnDataLoss", "false") \
    .load() \
    .withColumn("value", col("value").cast("string"))

speed_df = df.select(
    split(col("value"), ',')[0].alias("time_str"),
    split(col("value"), ',')[1].alias("speed_str")
)


speed_df = speed_df.withColumn("time", to_timestamp(col("time_str")))
speed_df = speed_df.withColumn("speed", col("speed_str").cast("float"))


avg_speed_df = speed_df \
    .withWatermark("time", "1 seconds") \
    .groupBy(window("time", "5 seconds", "5 seconds")) \
    .agg(avg("speed").alias("avg_speed"))


# Preparing to write
avg_speed_df = avg_speed_df.withColumn("avg_speed", round(col("avg_speed"), 4))
avg_speed_df = avg_speed_df.withColumn("avg_speed_str", col("avg_speed").cast("string"))

avg_speed_df = avg_speed_df.withColumn("time_end", col("window.end"))
# Because window is exclusive on right end, to align avg speed with
# time stamp till which is calculated  1 second is subtracted
avg_speed_df = avg_speed_df.withColumn("time_end", expr("time_end - interval 1 second"))
avg_speed_df = avg_speed_df.withColumn("time_str", date_format("time_end", "yyyy-MM-dd HH:mm:ss"))

avg_speed_df = avg_speed_df.withColumn("value", concat(col("time_str"), lit(","), col("avg_speed_str")))

# Write to kafka
write_kafka_params = {
    "kafka.bootstrap.servers": "kafka1:19092,kafka2:19093,kafka3:19094",
    "topic": "avg_speed"
}


kafka_query = avg_speed_df.writeStream \
    .outputMode("append") \
    .format("kafka") \
    .options(**write_kafka_params) \
    .option("checkpointLocation", "hdfs://namenode:9000/tmp/avg_speed_streamer/") \
    .start()

# Write to postgres
def write_to_postgres(micro_batch_df, batch_id):
    # JDBC connection properties
    jdbc_url = "jdbc:postgresql://citus-coordinator:5432/running-analytics"
    pg_properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }

    # Write the micro-batch DataFrame to PostgreSQL
    micro_batch_df.select(col("window.end").alias("time"), col("avg_speed").alias("avg_speed")).write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "avg_speed") \
        .option("user", pg_properties["user"]) \
        .option("password", pg_properties["password"]) \
        .option("driver", pg_properties["driver"]) \
        .mode("append") \
        .save()

pg_query = avg_speed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

# Write to hdfs
avg_speed_df.select(col("value")) \
    .writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "hdfs://namenode:9000/streams_csv/avg_speed") \
    .option("checkpointLocation", "/tmp/csv/avg_speed") \
    .start()


# Wait for the streaming query to finish
spark.streams.awaitAnyTermination()
