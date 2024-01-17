from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, window, avg, current_timestamp, coalesce, lit, expr

conf = SparkConf() \
    .setAppName("current-speed-comp-streamer") \
    .setMaster("spark://spark-master:7077") \
    .set("spark.cores.max", "4") \
    .set("spark.executor.cores", "2") \

spark = SparkSession.builder \
    .config(conf=conf) \
    .getOrCreate()


kafka_speed_input_options = {
    "kafka.bootstrap.servers": "kafka1:19092,kafka2:19093,kafka3:19094",
    "subscribe": "speed"
}

speed_df = spark.readStream \
    .format("kafka") \
    .options(**kafka_speed_input_options) \
    .option("failOnDataLoss", "false") \
    .load() \
    .withColumn("speed", col("value").cast("string")) \
    .withColumn("speed", col("speed").cast("float")) \
    .withColumn("timestamp", current_timestamp())

kafka_avg_speed_input_options = {
    "kafka.bootstrap.servers": "kafka1:19092,kafka2:19093,kafka3:19094",
    "subscribe": "avg_speed"
}

avg_speed_df = spark.readStream \
    .format("kafka") \
    .options(**kafka_avg_speed_input_options) \
    .option("failOnDataLoss", "false") \
    .load() \
    .withColumn("avg_speed", col("value").cast("string")) \
    .withColumn("avg_speed", col("avg_speed").cast("float")) \
    .withColumn("timestamp", current_timestamp())

joined_df = speed_df.join(avg_speed_df, "timestamp")

# Compare speed and avg_speed, then determine status
status_df = joined_df.withColumn(
    "value",
    expr("CASE WHEN speed > avg_speed THEN 'faster' ELSE 'slower' END")
)


write_kafka_params = {
    "kafka.bootstrap.servers": "kafka1:19092,kafka2:19093,kafka3:19094",
    "topic": "current_speed_comp"
}


query = status_df.writeStream \
    .outputMode("append") \
    .format("kafka") \
    .options(**write_kafka_params) \
    .option("checkpointLocation", "/tmp/current_speed_comp_streamer/") \
    .start()


# Wait for the streaming query to finish
query.awaitTermination()
