from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, window, avg, current_timestamp, split, to_timestamp, date_format, concat, \
    lit, expr

# Cluster config
conf = SparkConf() \
    .setAppName("pace-streamer") \
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


# Define the windowed aggregation
pace_df = speed_df \
    .withColumn("pace", lit(16.666666667) / col("speed"))

# Preparing to write
pace_df = pace_df.withColumn("pace", round(col("pace"), 4))
pace_df = pace_df.withColumn("pace_str", col("pace").cast("string"))

pace_df = pace_df.withColumn("time_str", date_format("time", "yyyy-MM-dd HH:mm:ss"))

pace_df = pace_df.withColumn("value", concat(col("time_str"), lit(","), col("pace_str")))

# Define Kafka parameters for writing to the "speed" topic
write_kafka_params = {
    "kafka.bootstrap.servers": "kafka1:19092,kafka2:19093,kafka3:19094",
    "topic": "pace"
}


kafka_query = pace_df.writeStream \
    .outputMode("append") \
    .format("kafka") \
    .options(**write_kafka_params) \
    .option("checkpointLocation", "hdfs://namenode:9000/tmp/pace_streamer/") \
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
    micro_batch_df.select(col("time").alias("time"), col("pace").alias("pace")).write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "pace") \
        .option("user", pg_properties["user"]) \
        .option("password", pg_properties["password"]) \
        .option("driver", pg_properties["driver"]) \
        .mode("append") \
        .save()

# Start the streaming query with foreachBatch
pg_query = pace_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()


# Write to hdfs
pace_df.select(col("value")) \
    .writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "hdfs://namenode:9000/streams_csv/pace") \
    .option("checkpointLocation", "/tmp/csv/pace") \
    .start()

spark.streams.awaitAnyTermination()
