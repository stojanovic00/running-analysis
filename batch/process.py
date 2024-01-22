from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, monotonically_increasing_id


conf = SparkConf() \
    .setAppName("batch-processing") \
    .setMaster("spark://spark-master:7077")

spark = SparkSession.builder \
    .config(conf=conf) \
    .getOrCreate()


jdbc_url = "jdbc:postgresql://citus-coordinator:5432/running-analytics"
pg_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# Loading dataset and initial dataframe preparation
df = spark.read.csv("hdfs://namenode:9000/user/aleksandar/data/ultramarathons.csv", header=True, inferSchema=True)
# df = spark.read.csv("hdfs://namenode:9000/user/aleksandar/data/batch_sample.csv", header=True, inferSchema=True)

df = df.select([col(c).alias(c.replace(" ", "_")) for c in df.columns])
df = df.select([col(c).alias(c.lower()) for c in df.columns])

df = df.withColumn("athlete_age", col("year_of_event").cast("int") - col("athlete_year_of_birth").cast("int"))
df = df.drop("athlete_age_category")


# Separating races to types and processing time  constrained df
marathon_type_filter = (col("event_distance/length").contains("d") |
                        col("event_distance/length").contains("h"))
# Create two separate DataFrames based on the condition
time_constrained_df = df.filter(marathon_type_filter)
distance_constrained_df = df.filter(~marathon_type_filter)

# TIME CONSTRAINED MARATHONS PROCESSING
time_constrained_df = time_constrained_df.withColumnRenamed("event_distance/length", "time_limit")
time_constrained_df = time_constrained_df.withColumnRenamed("athlete_performance", "distance_crossed")

# Normalize time to hours
days_expr = "(CASE WHEN time_limit LIKE '%d%' THEN CAST(regexp_extract(time_limit, '([0-9]+)', 1) AS INT) ELSE 0 END) * 24"
hours_expr = "(CASE WHEN time_limit LIKE '%h%' THEN CAST(regexp_extract(time_limit, '([0-9]+)', 1) AS INT) ELSE 0 END)"

time_constrained_df = time_constrained_df.withColumn("time_limit_h", expr(f"{days_expr} + {hours_expr}"))
time_constrained_df = time_constrained_df.drop("time_limit")

# Normalize crossed distance to km
distance_expr = """
                  ROUND(
                  CASE 
                  WHEN distance_crossed LIKE '%km%' THEN CAST(regexp_extract(distance_crossed, '([0-9]+(.[0-9]+)?)', 1) AS FLOAT) 
                  WHEN distance_crossed LIKE '%mi%' THEN CAST(regexp_extract(distance_crossed, '([0-9]+(.[0-9]+)?)', 1) AS FLOAT) *1.609344 
                  ELSE 0 
                  END, 4)
              """

time_constrained_df = time_constrained_df.withColumn("distance_crossed_km", expr(f"{distance_expr}"))
time_constrained_df = time_constrained_df.drop("distance_crossed")

time_constrained_df = time_constrained_df.withColumn("athlete_year_of_birth", col("athlete_year_of_birth").cast("int"))

time_constrained_df = time_constrained_df.withColumn("athlete_average_speed_kmph", col("athlete_average_speed").cast("float") / 1000)
time_constrained_df = time_constrained_df.drop("athlete_average_speed")

# Add an artificial key column
time_constrained_df = time_constrained_df.withColumn("id", monotonically_increasing_id())


# Writing time_constrained to db
time_constrained_df.write.jdbc(jdbc_url, "public.running_time_constrained", mode="overwrite", properties=pg_properties)



# DISTANCE CONSTRAINED MARATHONS PROCESSING
distance_constrained_df = distance_constrained_df.withColumnRenamed("event_distance/length", "distance")
distance_constrained_df = distance_constrained_df.withColumnRenamed("athlete_performance", "time_spent")

# Normalizing distance to km
distance_expr = """
                  ROUND(
                  CASE 
                  WHEN distance LIKE '%km%' THEN CAST(regexp_extract(distance, '([0-9]+(.[0-9]+)?)', 1) AS FLOAT) 
                  WHEN distance LIKE '%mi%' THEN CAST(regexp_extract(distance, '([0-9]+(.[0-9]+)?)', 1) AS FLOAT) *1.609344 
                  ELSE 0 
                  END, 4)
              """


distance_constrained_df = distance_constrained_df.withColumn("distance_km", expr(f"{distance_expr}"))
distance_constrained_df = distance_constrained_df.drop("distance")

# Normalizing time to seconds for easier manipulation later
# Extract components
days_expr = "CASE WHEN time_spent LIKE '%d%' THEN CAST(regexp_extract(time_spent, '([0-9]+)d', 1) AS INT) ELSE 0 END"
hours_expr = "CAST(regexp_extract(time_spent, '([0-9]+):([0-9]{2}):([0-9]{2})', 1) AS INT)"
minutes_expr = "CAST(regexp_extract(time_spent, '([0-9]+):([0-9]{2}):([0-9]{2})', 2) AS INT)"
seconds_expr = "CAST(regexp_extract(time_spent, '([0-9]+):([0-9]{2}):([0-9]{2})', 3) AS INT)"

# Convert to seconds and sum up
total_seconds_expr = f"({days_expr} * 24 * 60 * 60) + ({hours_expr} * 60 * 60) + ({minutes_expr} * 60) + {seconds_expr}"

# Add a new column 'total_duration_seconds'
distance_constrained_df = distance_constrained_df.withColumn("time_spent_s", expr(total_seconds_expr))


# Normalize average speed to km/h
distance_constrained_df = distance_constrained_df.withColumn(
    "athlete_average_speed_kmph",
    when(col("athlete_average_speed").cast("double") > 50, col("athlete_average_speed").cast("double") / 1000).
    otherwise(col("athlete_average_speed").cast("double"))
)
distance_constrained_df = distance_constrained_df.drop("athlete_average_speed")

# Add an artificial key column
distance_constrained_df = distance_constrained_df.withColumn("id", monotonically_increasing_id())


# Grouping marathons depending on size

dist_groups = [
    (0, 40), (40, 65), (65, 90), (90, 120),
    (120, 200), (200, 1000), (1000, 6000)
]

# Create a new column representing the distance group
distance_constrained_df = distance_constrained_df.withColumn(
    "distance_group_km",
    when(
        (col("distance_km").isNull() | (col("distance_km") <= 0)),
        None
    ).otherwise(  # This makes case for every distance group
    expr(
        """
        CASE {}
        ELSE '{}'
        END
        """.format(
            "".join(
                [
                    "WHEN distance_km >= {} AND distance_km < {} THEN '{}'\n".format(lower, upper, f"{lower}-{upper}")
                    for lower, upper in dist_groups
                ]
            ),
            None
        )
    )
)
)

# Calculating age group for runner
dist_groups = [
    (17, 30), (30, 45), (45, 120)
]
distance_constrained_df = distance_constrained_df.withColumn(
    "age_group",
    when(
        (col("athlete_age").isNull() | (col("athlete_age") <= 0)),
        None
    ).otherwise(  # This makes case for every distance group
        expr(
            """
            CASE {}
            ELSE '{}'
            END
            """.format(
                "".join(
                    [
                        "WHEN athlete_age >= {} AND athlete_age < {} THEN '{}'\n".format(lower, upper, f"{lower}-{upper}")
                        for lower, upper in dist_groups
                    ]
                ),
                None
            )
        )
    )
)
# Writing distance_constrained to db
distance_constrained_df.write.jdbc(jdbc_url, "public.running_distance_constrained", mode="overwrite", properties=pg_properties)