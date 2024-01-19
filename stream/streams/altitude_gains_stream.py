from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, split, date_format, concat, lit, to_timestamp, expr

conf = SparkConf() \
    .setAppName("altitude-gains-streamer") \
    .setMaster("spark://spark-master:7077") \
    .set("spark.cores.max", "4") \
    .set("spark.executor.cores", "2")

spark = SparkSession.builder \
    .config(conf=conf) \
    .getOrCreate()


# Define Kafka parameters
kafka_input_options = {
    "kafka.bootstrap.servers": "kafka1:19092,kafka2:19093,kafka3:19094",
    "subscribe": "run_stats"
}

# current
# Read from Kafka using the Kafka source for Spark Structured Streaming
df_current = spark.readStream \
    .format("kafka") \
    .options(**kafka_input_options) \
    .option("failOnDataLoss", "false") \
    .load()

csv_current_df = df_current.select(col("value").cast("string"))

# Extract altitude and keep timestamp
altitude_current_df = csv_current_df.select(
    split(col("value"), ',')[0].alias("time_str"),
    split(col("value"), ',')[3].alias("altitude_str")
)

altitude_current_df = altitude_current_df.withColumn("time", to_timestamp(col("time_str")))
altitude_current_df = altitude_current_df.withColumn("time_str", date_format("time", "yyyy-MM-dd HH:mm:ss"))

# Round altitude to 4 decimals
altitude_current_df = altitude_current_df.withColumn("altitude", col("altitude_str").cast("float"))
altitude_current_df = altitude_current_df.withColumn("altitude_current", round(col("altitude"), 4))

# PRECEDING
# Read from Kafka using the Kafka source for Spark Structured Streaming
df_preceding = spark.readStream \
    .format("kafka") \
    .options(**kafka_input_options) \
    .option("failOnDataLoss", "false") \
    .load()

csv_preceding_df = df_preceding.select(col("value").cast("string"))

# Extract altitude and keep timestamp
altitude_preceding_df = csv_preceding_df.select(
    split(col("value"), ',')[0].alias("time_str"),
    split(col("value"), ',')[3].alias("altitude_str")
)
# Translate time forward by 1s so I can join them by date and by doing that I will get
# pair of current, preceding(+1 second translated)
altitude_preceding_df = altitude_preceding_df.withColumn("time", to_timestamp(col("time_str")))
altitude_preceding_df = altitude_preceding_df.withColumn("time", expr("time + interval 1 second"))
altitude_preceding_df = altitude_preceding_df.withColumn("time_str", date_format("time", "yyyy-MM-dd HH:mm:ss"))

# Round altitude to 4 decimals
altitude_preceding_df = altitude_preceding_df.withColumn("altitude", col("altitude_str").cast("float"))
altitude_preceding_df = altitude_preceding_df.withColumn("altitude_preceding", round(col("altitude"), 4))


joined_df = altitude_current_df.join(altitude_preceding_df, "time_str")

# Calculate altitude gain
joined_df = joined_df.withColumn("altitude_gain",
         expr("CASE WHEN altitude_current > altitude_preceding THEN altitude_current - altitude_preceding ELSE 0 END"))
# Just rounds
joined_df = joined_df.withColumn("altitude_gain", round(col("altitude_gain"), 4))


joined_df = joined_df.withColumn("value", concat(col("time_str"), lit(","), col("altitude_gain")))

# Define Kafka parameters for writing to the "speed" topic
write_kafka_params = {
    "kafka.bootstrap.servers": "kafka1:19092,kafka2:19093,kafka3:19094",
    "topic": "altitude_gains"
}


# Write the speed data to the "speed" topic
query = joined_df.writeStream \
    .outputMode("append") \
    .format("kafka") \
    .options(**write_kafka_params) \
    .option("checkpointLocation", "hdfs://namenode:9000/tmp/altitude_gains_streamer/") \
    .start()


# Wait for the streaming query to finish
query.awaitTermination()
