from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, split, to_timestamp, date_format, concat, lit, expr

# Cluster config
conf = SparkConf() \
    .setAppName("finish-estimation-streamer") \
    .setMaster("spark://spark-master:7077") \
    .set("spark.cores.max", "4") \
    .set("spark.executor.cores", "2")

spark = SparkSession.builder \
    .config(conf=conf) \
    .getOrCreate()


kafka_input_options = {
    "kafka.bootstrap.servers": "kafka1:19092,kafka2:19093,kafka3:19094",
    "subscribe": "run_stats"
}

df = spark.readStream \
    .format("kafka") \
    .options(**kafka_input_options) \
    .option("failOnDataLoss", "false") \
    .load()

csv_df = df.select(col("value").cast("string"))

# Extract distance (meters) and time
estimation_df = csv_df.select(
    split(col("value"), ',')[0].alias("time_str"),
    split(col("value"), ',')[4].alias("distance")
)

# Handling time
estimation_df = estimation_df.withColumn("time", to_timestamp(col("time_str")))

# Round speed to 4 decimals
estimation_df = estimation_df.withColumn("distance", col("distance").cast("float"))
estimation_df = estimation_df.withColumn("distance", round(col("distance"), 4))

# WARNING
# Load historical data
jdbc_url = "jdbc:postgresql://citus-coordinator:5432/running-analytics"
pg_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver",
}



# Calculate avg speed but recent years have more weight

historical_df = spark.read \
    .jdbc(url=jdbc_url,
          table="running_distance_constrained",
          properties=pg_properties) \
    .filter("athlete_id = 816711 and distance_km = 21 and year_of_event <= 2022") \
    .select("year_of_event", "athlete_average_speed_kmph")

current_year = 2022
marathon_length_km = 21
max_years = current_year - historical_df.agg({"year_of_event": "min"}).collect()[0][0] + 1

weighted_sum = historical_df.withColumn(
    "weight",
    lit(1) - (current_year - col("year_of_event")) / max_years) \
 .withColumn("avg_speed_weighted", col("athlete_average_speed_kmph") * col("weight")) \
.agg(
    {"avg_speed_weighted": "sum", "weight": "sum"}
).collect()


weighted_average = weighted_sum[0]["sum(avg_speed_weighted)"] / weighted_sum[0]["sum(weight)"]

estimation_df = estimation_df.withColumn("average_speed", lit(weighted_average))
estimation_df = estimation_df.withColumn("average_speed", round(col("average_speed"), 4))

# Estimate finish time

estimation_df = estimation_df.withColumn("marathon_length_km", lit(marathon_length_km))
estimation_df = estimation_df.withColumn("remaining_m", col("marathon_length_km") * 1000 - col("distance"))

estimation_df = estimation_df.withColumn("avg_speed_mpmin", col("average_speed") * (1000/60) )

estimation_df = estimation_df.withColumn("estimated_min", col("remaining_m") / col("avg_speed_mpmin") )

estimation_df = estimation_df.withColumn("estimated_min", round(col("estimated_min"), 4))


# Write to stream

estimation_df = estimation_df.withColumn("estimation_str", col("estimated_min").cast("string"))
estimation_df = estimation_df.withColumn("time_str", date_format("time", "yyyy-MM-dd HH:mm:ss"))
estimation_df = estimation_df.withColumn("value", concat(col("time_str"), lit(","), col("estimation_str"), lit(" min")))

# Define Kafka parameters for writing to the "speed" topic
write_kafka_params = {
    "kafka.bootstrap.servers": "kafka1:19092,kafka2:19093,kafka3:19094",
    "topic": "finish_estimation"
}

kafka_query = estimation_df.writeStream \
    .outputMode("append") \
    .format("kafka") \
    .options(**write_kafka_params) \
    .option("checkpointLocation", "hdfs://namenode:9000/tmp/finish_estimation_streamer/") \
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
    micro_batch_df.select(col("time").alias("time"), col("estimated_min").alias("estimated_min")).write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "finish_estimation") \
        .option("user", pg_properties["user"]) \
        .option("password", pg_properties["password"]) \
        .option("driver", pg_properties["driver"]) \
        .mode("append") \
        .save()

# Start the streaming query with foreachBatch
pg_query = estimation_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()



# Write to hdfs
estimation_df.select(col("value")) \
    .writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "hdfs://namenode:9000/streams_csv/estimation") \
    .option("checkpointLocation", "/tmp/csv/estimation") \
    .start()

# Wait for the streaming query to finish
spark.streams.awaitAnyTermination()
