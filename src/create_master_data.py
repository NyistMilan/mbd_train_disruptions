from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import broadcast

RAW_BASE = "/user/s3692612/final_project/data/raw"
MASTER_OUT = "/user/s3692612/final_project/data/master"

SERVICES_PATH = f"{RAW_BASE}/services/*"
DISRUPTIONS_PATH = f"{RAW_BASE}/disruptions/*"
STATIONS_PATH = f"{RAW_BASE}/stations/*"

spark = (
    SparkSession.builder
    .appName("create_master_data")
    .getOrCreate()
)

services = spark.read.option("header", True).csv(SERVICES_PATH)
disruptions = spark.read.option("header", True).csv(DISRUPTIONS_PATH)
stations = spark.read.option("header", True).csv(STATIONS_PATH)

# Clean Services
services = (
    services
    .withColumnRenamed("Service:RDT-ID", "service_rdt_id")
    .withColumnRenamed("Service:Date", "service_date")
    .withColumnRenamed("Service:Type", "service_type")
    .withColumnRenamed("Service:Company", "service_company")
    .withColumnRenamed("Service:Train number", "service_train_number")
    .withColumnRenamed("Service:Completely cancelled", "service_completely_cancelled")
    .withColumnRenamed("Service:Partly cancelled", "service_partly_cancelled")
    .withColumnRenamed("Service:Maximum delay", "service_maximum_delay")
    .withColumnRenamed("Stop:RDT-ID", "stop_rdt_id")
    .withColumnRenamed("Stop:Station code", "stop_station_code")
    .withColumnRenamed("Stop:Station name", "stop_station_name")
    .withColumnRenamed("Stop:Arrival time", "stop_arrival_time")
    .withColumnRenamed("Stop:Arrival delay", "stop_arrival_delay")
    .withColumnRenamed("Stop:Arrival cancelled", "stop_arrival_cancelled")
    .withColumnRenamed("Stop:Departure time", "stop_departure_time")
    .withColumnRenamed("Stop:Departure delay", "stop_departure_delay")
    .withColumnRenamed("Stop:Departure cancelled", "stop_departure_cancelled")
    .withColumnRenamed("Stop:Platform change", "stop_platform_change")
    .withColumnRenamed("Stop:Planned platform", "stop_planned_platform")
    .withColumnRenamed("Stop:Actual platform", "stop_actual_platform")
)

services = (
    services
    .withColumn("service_train_number", F.col("service_train_number").cast("int"))
    .withColumn("service_maximum_delay", F.col("service_maximum_delay").cast("int"))
    .withColumn("stop_arrival_delay", F.col("stop_arrival_delay").cast("double"))
    .withColumn("stop_departure_delay", F.col("stop_departure_delay").cast("double"))
    .withColumn("service_completely_cancelled", F.col("service_completely_cancelled").cast("boolean"))
    .withColumn("service_partly_cancelled", F.col("service_partly_cancelled").cast("boolean"))
    .withColumn("stop_arrival_cancelled", F.col("stop_arrival_cancelled").cast("boolean"))
    .withColumn("stop_departure_cancelled", F.col("stop_departure_cancelled").cast("boolean"))
    .withColumn("stop_platform_change", F.col("stop_platform_change").cast("boolean"))
    .withColumn("service_date", F.to_date("service_date"))
    .withColumn("stop_station_code", F.upper(F.trim(F.col("stop_station_code"))))
    .withColumn("stop_arrival_ts", F.to_timestamp("stop_arrival_time"))
    .withColumn("stop_departure_ts", F.to_timestamp("stop_departure_time"))
    .withColumn("stop_event_ts", F.coalesce(F.col("stop_departure_ts"), F.col("stop_arrival_ts")))
)

# Clean Stations
stations_dim = (
    stations
    .withColumnRenamed("geo_lat", "station_geo_lat")
    .withColumnRenamed("geo_lng", "station_geo_lng")
    .withColumnRenamed("code", "station_code")
    .withColumnRenamed("country", "station_country")
    .withColumn("station_code", F.upper(F.trim(F.col("station_code"))))
    .withColumn("station_geo_lat", F.col("station_geo_lat").cast("double"))
    .withColumn("station_geo_lng", F.col("station_geo_lng").cast("double"))
    .select("station_code", "station_country", "station_geo_lat", "station_geo_lng")
    .dropDuplicates(["station_code"])
)

# Clean Disruptions
disruptions_exploded = (
    disruptions
    .withColumnRenamed("rdt_station_codes", "disruption_station_code")
    .withColumnRenamed("cause_en", "disruption_cause_en")
    .withColumnRenamed("statistical_cause_en", "disruption_statistical_cause_en")
    .withColumnRenamed("cause_group", "disruption_cause_group")
    .withColumnRenamed("start_time", "disruption_start_time")
    .withColumnRenamed("end_time", "disruption_end_time")
    .select(
        "disruption_station_code",
        "disruption_cause_en",
        "disruption_statistical_cause_en",
        "disruption_cause_group",
        "disruption_start_time",
        "disruption_end_time",
    )
    .withColumn("disruption_start_time_ts", F.to_timestamp("disruption_start_time"))
    .withColumn("disruption_end_time_ts", F.to_timestamp("disruption_end_time"))
    .withColumn(
        "disruption_station_code",
        F.explode(
            F.split(
                F.regexp_replace(F.coalesce(F.col("disruption_station_code"), F.lit("")), r"\s+", ""),
                ",",
            )
        ),
    )
    .withColumn("disruption_station_code", F.upper(F.trim(F.col("disruption_station_code"))))
    .filter(F.col("disruption_station_code") != "")
    .select(
        "disruption_station_code",
        "disruption_cause_en",
        "disruption_statistical_cause_en",
        "disruption_cause_group",
        "disruption_start_time_ts",
        "disruption_end_time_ts",
    )
    .dropDuplicates([
        "disruption_station_code",
        "disruption_cause_en",
        "disruption_statistical_cause_en",
        "disruption_cause_group",
        "disruption_start_time_ts",
        "disruption_end_time_ts",
    ])
)

# Joins
master = (
    services.alias("s")
    .join(broadcast(stations_dim).alias("st"),
          F.col("s.stop_station_code") == F.col("st.station_code"),
          "left")
    .join(
        disruptions_exploded.alias("d"),
        (F.col("s.stop_station_code") == F.col("d.disruption_station_code")) &
        F.col("s.stop_event_ts").isNotNull() &
        F.col("d.disruption_start_time_ts").isNotNull() &
        F.col("d.disruption_end_time_ts").isNotNull() &
        (F.col("s.stop_event_ts") >= F.col("d.disruption_start_time_ts")) &
        (F.col("s.stop_event_ts") <= F.col("d.disruption_end_time_ts")),
        "left",
    )
    .drop("station_code")
)

master = master.withColumn("year", F.year("service_date"))

(
    master.write
    .mode("overwrite")
    .option("compression", "snappy")
    .partitionBy("year")
    .parquet(MASTER_OUT)
)