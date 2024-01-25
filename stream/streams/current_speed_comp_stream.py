from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, window, avg, current_timestamp, coalesce, lit, expr, date_format, \
    from_unixtime, to_date, split, to_timestamp, concat

conf = SparkConf() \
    .setAppName("current-speed-comp-streamer") \
    .setMaster("spark://spark-master:7077") \
    .set("spark.cores.max", "4") \
    .set("spark.executor.cores", "2")

spark = SparkSession.builder \
    .config(conf=conf) \
    .getOrCreate()

# SPEED
kafka_speed_input_options = {
    "kafka.bootstrap.servers": "kafka1:19092,kafka2:19093,kafka3:19094",
    "subscribe": "speed"
}

df = spark.readStream \
    .format("kafka") \
    .options(**kafka_speed_input_options) \
    .option("failOnDataLoss", "false") \
    .load() \

speed_df = df.select(
    split(col("value"), ',')[0].alias("time_str"),
    split(col("value"), ',')[1].alias("speed_str")
)

speed_df = speed_df.withColumn("time", to_timestamp(col("time_str")))
speed_df = speed_df.withColumn("speed", col("speed_str").cast("float"))

# AVG SPEED
kafka_avg_speed_input_options = {
    "kafka.bootstrap.servers": "kafka1:19092,kafka2:19093,kafka3:19094",
    "subscribe": "avg_speed"
}

df = spark.readStream \
    .format("kafka") \
    .options(**kafka_avg_speed_input_options) \
    .option("failOnDataLoss", "false") \
    .load() \

avg_speed_df = df.select(
    split(col("value"), ',')[0].alias("time_str"),
    split(col("value"), ',')[1].alias("avg_speed_str")
)

avg_speed_df = avg_speed_df.withColumn("time", to_timestamp(col("time_str")))
avg_speed_df = avg_speed_df.withColumn("avg_speed", col("avg_speed_str").cast("float"))

joined_df = speed_df.join(avg_speed_df, "time")

# Compare speed and avg_speed, then determine status
status_df = joined_df.withColumn(
    "comparison",
    expr("CASE WHEN speed > avg_speed THEN 'faster' ELSE 'slower' END")
)

status_df = status_df.withColumn("time_string", date_format("time", "yyyy-MM-dd HH:mm:ss"))
status_df = status_df.withColumn("value", concat(col("time_string"), lit(","), col("comparison")))

write_kafka_params = {
    "kafka.bootstrap.servers": "kafka1:19092,kafka2:19093,kafka3:19094",
    "topic": "current_speed_comp"
}

kafka_query = status_df.writeStream \
    .outputMode("append") \
    .format("kafka") \
    .options(**write_kafka_params) \
    .option("checkpointLocation", "hdfs://namenode:9000/tmp/current_speed_comp_streamer/") \
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
    micro_batch_df.select(col("time").alias("time"), col("comparison").alias("comparison")).write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "comparison") \
        .option("user", pg_properties["user"]) \
        .option("password", pg_properties["password"]) \
        .option("driver", pg_properties["driver"]) \
        .mode("append") \
        .save()

# Start the streaming query with foreachBatch
pg_query = status_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

# Write to hdfs
status_df.select(col("value")) \
    .writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "hdfs://namenode:9000/streams_csv/speed_comp") \
    .option("checkpointLocation", "/tmp/csv/speed_comp") \
    .start()


# Wait for the streaming query to finish
spark.streams.awaitAnyTermination()
