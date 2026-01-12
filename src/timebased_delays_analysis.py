from pyspark.sql import SparkSession, functions as F

MASTER_PATH = "/user/s3692612/final_project/data/master"
ANALYSIS_OUT = "/user/s3082474/final_project/data/analysis/timebased_delays"
HOLIDAYS_CSV = "/user/s3082474/final_project/holidays.csv"

spark = (
    SparkSession.builder
    .appName("analyze_delays")
    .getOrCreate()
)

master = spark.read.parquet(MASTER_PATH)

# Prepare dataframe
df = (
    master
    .select(
        "service_date",
        "service_completely_cancelled",
        "service_partly_cancelled",
        "stop_arrival_ts",
        "stop_departure_ts",
        "stop_arrival_delay",
        "stop_departure_delay",
    )
    .withColumn("event_ts", F.coalesce(F.col("stop_arrival_ts"), F.col("stop_departure_ts")))
    .withColumn("event_date", F.to_date(F.col("event_ts")))
    .withColumn("hour", F.hour(F.col("event_ts")))
    .withColumn("delay_val", F.coalesce(F.col("stop_arrival_delay"), F.col("stop_departure_delay")))
    .withColumn("delay_val_filled", F.coalesce(F.col("delay_val"), F.lit(0.0)).cast("double"))
    .filter(F.col("event_ts").isNotNull())
    .cache()
)

# Number of trains per day
daily_totals = df.groupBy("event_date").agg(F.count(F.lit(1)).alias("trains_in_day"))

# Hourly aggregates
hourly = (
    df
    .groupBy("event_date", "hour")
    .agg(
        F.count(F.lit(1)).alias("trains_in_hour"),
        F.sum(F.when(F.col("service_completely_cancelled") == True, 1).otherwise(0)).alias("completely_cancelled"),
        F.sum(F.when(F.col("service_partly_cancelled") == True, 1).otherwise(0)).alias("partly_cancelled"),
        F.sum(F.when(F.col("delay_val_filled") > 0, 1).otherwise(0)).alias("delayed_count"),
        F.sum(F.when(F.col("delay_val_filled") == 0, 1).otherwise(0)).alias("on_time_count"),
        F.avg(F.col("delay_val_filled")).alias("avg_delay_all_trains"),
        F.avg(F.when(F.col("delay_val") > 0, F.col("delay_val")).cast("double")).alias("avg_delay_delays_only"),
    )
)

# Join with daily totals to compute the hour share
hourly_with_day = (
    hourly.join(daily_totals, ["event_date"], "left")
    .withColumn("hour_share_of_day", F.col("trains_in_hour") / F.col("trains_in_day"))
    .select(
        F.col("event_date").alias("service_date"),
        "hour",
        "trains_in_hour",
        "trains_in_day",
        "hour_share_of_day",
        "completely_cancelled",
        "partly_cancelled",
        "delayed_count",
        "on_time_count",
        F.when(F.col("trains_in_hour") > 0, (F.col("completely_cancelled") / F.col("trains_in_hour")).cast("double")).otherwise(F.lit(0.0)).alias("pct_completely_cancelled"),
        F.when(F.col("trains_in_hour") > 0, (F.col("partly_cancelled") / F.col("trains_in_hour")).cast("double")).otherwise(F.lit(0.0)).alias("pct_partly_cancelled"),
        F.when(F.col("trains_in_hour") > 0, (F.col("delayed_count") / F.col("trains_in_hour")).cast("double")).otherwise(F.lit(0.0)).alias("pct_delayed"),
        F.when(F.col("trains_in_hour") > 0, (F.col("on_time_count") / F.col("trains_in_hour")).cast("double")).otherwise(F.lit(0.0)).alias("pct_on_time"),
        "avg_delay_all_trains",
        "avg_delay_delays_only",
    )
)

# Read holidays from CSV (expects columns: event_date, holiday)
holidays_spark = (
    spark.read.option("header", "true").csv(HOLIDAYS_CSV)
    .withColumn("event_date", F.to_date(F.col("event_date")))
)

# join on service_date to attach holiday name
hourly_with_holiday = (
    hourly_with_day.join(holidays_spark, hourly_with_day.service_date == holidays_spark.event_date, "left")
    .withColumn("holiday", F.col("holiday"))
    .withColumn("is_holiday", F.col("holiday").isNotNull())
    .select(*[c for c in hourly_with_day.columns], "holiday", "is_holiday")
)

# Output the result (include holiday join)
hourly_with_holiday.coalesce(1).write.mode("overwrite").option("header", True).csv(ANALYSIS_OUT)
spark.stop()