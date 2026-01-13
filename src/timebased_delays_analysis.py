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
        "service_rdt_id",
        "service_completely_cancelled",
        "service_partly_cancelled",
        "stop_arrival_ts",
        "stop_departure_ts",
        "stop_arrival_delay",
        "stop_departure_delay",
    )
    .withColumn("event_ts", F.coalesce(F.col("stop_arrival_ts"), F.col("stop_departure_ts")))
    .filter(F.col("event_ts").isNotNull())
    .withColumn("service_date", F.to_date(F.col("event_ts")))
    .filter(~((F.year(F.col("service_date")) == 2025) & (F.month(F.col("service_date")) == 12)))
    .withColumn("hour", F.hour(F.col("event_ts")))
    .withColumn("missing_event_ts", F.lit(False))
    .withColumn("delay_val", F.coalesce(F.col("stop_arrival_delay"), F.col("stop_departure_delay")))
    .withColumn("delay_val_filled", F.coalesce(F.col("delay_val"), F.lit(0.0)).cast("double"))
    .cache()
)

# Collapse to one row per service per hour
services_per_hour = (
    df
    .select(
        "service_rdt_id",
        "service_date",
        "hour",
        "service_completely_cancelled",
        "service_partly_cancelled",
        "delay_val_filled",
    )
    .groupBy("service_date", "hour", "service_rdt_id")
    .agg(
        F.max(F.col("service_completely_cancelled").cast("int")).alias("svc_completely_cancelled"),
        F.max(F.col("service_partly_cancelled").cast("int")).alias("svc_partly_cancelled"),
        F.max(F.col("delay_val_filled")).alias("svc_max_delay"),
        F.avg(F.col("delay_val_filled")).alias("svc_avg_delay"),
    )
)

# Flags per service: short delay (<3 min) and long delay (>10 min)
services_per_hour = services_per_hour.withColumn(
    "svc_short_delay",
    (F.col("svc_max_delay") > 0) & (F.col("svc_max_delay") < 3)
).withColumn(
    "svc_long_delay",
    F.col("svc_max_delay") > 10
)

# Number of distinct services per day
daily_totals = (
    services_per_hour
    .select("service_date", "service_rdt_id")
    .dropDuplicates(["service_date", "service_rdt_id"])
    .groupBy("service_date")
    .agg(F.count("service_rdt_id").alias("services_in_day"))
)

# Hourly aggregates across services
hourly = (
    services_per_hour
    .groupBy("service_date", "hour")
    .agg(
        F.count("service_rdt_id").alias("services_in_hour"),
        F.sum(F.col("svc_completely_cancelled")).alias("completely_cancelled"),
        F.sum(F.when(F.col("svc_completely_cancelled") == 1, 0).otherwise(F.col("svc_partly_cancelled"))).alias("partly_cancelled"),
        F.sum(F.when(F.col("svc_max_delay") > 0, 1).otherwise(0)).alias("delayed_count"),
        F.sum(F.when(F.col("svc_max_delay") == 0, 1).otherwise(0)).alias("on_time_count"),
        F.sum(F.when(F.col("svc_short_delay"), 1).otherwise(0)).alias("short_delay_count"),
        F.sum(F.when(F.col("svc_long_delay"), 1).otherwise(0)).alias("long_delay_count"),
        F.avg(F.col("svc_avg_delay")).alias("avg_avg_delay_all_services"),
        F.avg(F.when(F.col("svc_avg_delay") > 0, F.col("svc_avg_delay")).cast("double")).alias("avg_avg_delay_delays_only"),
    )
)

# Join with daily totals to compute the hour share
hourly_with_day = (
    hourly.join(daily_totals, ["service_date"], "left")
    .withColumn("hour_share_of_day", F.col("services_in_hour") / F.col("services_in_day"))
    .select(
        F.when(
            ((F.month(F.col("service_date")) == 12) & (F.dayofmonth(F.col("service_date")) >= 21)) |
            (F.month(F.col("service_date")).isin(1, 2)) |
            ((F.month(F.col("service_date")) == 3) & (F.dayofmonth(F.col("service_date")) <= 19)),
            F.lit("winter")
        ).when(
            ((F.month(F.col("service_date")) == 3) & (F.dayofmonth(F.col("service_date")) >= 20)) |
            (F.month(F.col("service_date")).isin(4, 5)) |
            ((F.month(F.col("service_date")) == 6) & (F.dayofmonth(F.col("service_date")) <= 20)),
            F.lit("spring")
        ).when(
            ((F.month(F.col("service_date")) == 6) & (F.dayofmonth(F.col("service_date")) >= 21)) |
            (F.month(F.col("service_date")).isin(7, 8)) |
            ((F.month(F.col("service_date")) == 9) & (F.dayofmonth(F.col("service_date")) <= 22)),
            F.lit("summer")
        ).when(
            ((F.month(F.col("service_date")) == 9) & (F.dayofmonth(F.col("service_date")) >= 23)) |
            (F.month(F.col("service_date")).isin(10, 11)) |
            ((F.month(F.col("service_date")) == 12) & (F.dayofmonth(F.col("service_date")) <= 20)),
            F.lit("autumn")
        ).alias("season"),
        F.col("service_date"),
        "hour",
        "hour_share_of_day",
        F.when(F.col("services_in_hour") > 0, (F.col("completely_cancelled") / F.col("services_in_hour")).cast("double")).otherwise(F.lit(0.0)).alias("pct_completely_cancelled"),
        F.when(F.col("services_in_hour") > 0, (F.col("partly_cancelled") / F.col("services_in_hour")).cast("double")).otherwise(F.lit(0.0)).alias("pct_partly_cancelled"),
        F.when(F.col("services_in_hour") > 0, (F.col("delayed_count") / F.col("services_in_hour")).cast("double")).otherwise(F.lit(0.0)).alias("pct_delayed"),
        F.when(F.col("services_in_hour") > 0, (F.col("on_time_count") / F.col("services_in_hour")).cast("double")).otherwise(F.lit(0.0)).alias("pct_on_time"),
        F.when(F.col("services_in_hour") > 0, (F.col("short_delay_count") / F.col("services_in_hour")).cast("double")).otherwise(F.lit(0.0)).alias("pct_short_delay"),
        F.when(F.col("services_in_hour") > 0, (F.col("long_delay_count") / F.col("services_in_hour")).cast("double")).otherwise(F.lit(0.0)).alias("pct_long_delay"),
        "avg_avg_delay_all_services",
        "avg_avg_delay_delays_only",
        "services_in_hour",
        "services_in_day",
        "completely_cancelled",
        "partly_cancelled",
        "delayed_count",
        "on_time_count",
        "short_delay_count",
        "long_delay_count",
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