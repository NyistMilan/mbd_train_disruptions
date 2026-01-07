from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import broadcast, col, radians, sin, cos, sqrt, atan2
from pyspark.sql.window import Window

# === Configuration ===
RAW_BASE = "/user/s3692612/final_project/data/raw"
MASTER_BASE = "/user/s3692612/final_project/data/master"
WEATHER_PATH = f"{RAW_BASE}/weather/*"
STATIONS_PATH = "/user/s3544648/final_project/data/raw/stations_consolidated/*"
SERVICES_PATH = MASTER_BASE  # Master data is directly here with year partitions
OUTPUT_PATH = "final_project/data/master/services_with_weather"

# Weather columns to keep (most relevant for delay analysis)
# Based on actual KNMI data schema
WEATHER_COLS_TO_KEEP = [
    "station",
    "time",
    "lat",
    "lon",
    "stationname",
    # Temperature
    "T",  # Air temperature (0.1 °C)
    "T10N",  # Minimum temperature at 10cm (0.1 °C)
    "TD",  # Dew point temperature (0.1 °C)
    # Humidity
    "U",  # Relative humidity (%)
    "RH",  # Relative humidity (alternative)
    # Precipitation
    "DR",  # Precipitation duration (0.1 hour)
    # Wind
    "DD",  # Wind direction (degrees)
    "FF",  # Mean wind speed (0.1 m/s)
    "FH",  # Hourly mean wind speed (0.1 m/s)
    "FX",  # Maximum wind gust (0.1 m/s)
    # Visibility & clouds
    "VV",  # Visibility (0-49: code, 50-89: km)
    "N",  # Cloud cover (octants)
    # Weather phenomena
    "WW",  # Present weather code
    # Pressure
    "P",  # Air pressure (0.1 hPa)
    # Sunshine
    "SQ",  # Sunshine duration (0.1 hour)
    "Q",  # Global radiation (J/cm2)
]

spark = (
    SparkSession.builder.appName("merge_weather_services")
    .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
    .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    .config("spark.sql.legacy.parquet.nanosAsLong", "true")
    .getOrCreate()
)


def euclidean_distance_squared(lat1, lon1, lat2, lon2):
    """Squared Euclidean distance - sufficient for finding nearest station in NL."""
    return (lat1 - lat2) ** 2 + (lon1 - lon2) ** 2


def haversine_distance_km(lat1, lon1, lat2, lon2):
    EARTH_RADIUS_KM = 6371.0
    lat1_rad = radians(lat1)
    lat2_rad = radians(lat2)
    lon1_rad = radians(lon1)
    lon2_rad = radians(lon2)
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = sin(dlat / 2) ** 2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return c * EARTH_RADIUS_KM


# === Load Data ===
print("Loading train stations data...")
stations = (
    spark.read.option("header", True)
    .parquet(STATIONS_PATH)
    .withColumnRenamed("code", "station_code")
    .withColumnRenamed("geo_lat", "station_lat")
    .withColumnRenamed("geo_lng", "station_lng")
    .withColumn("station_code", F.upper(F.trim(col("station_code"))))
    .withColumn("station_lat", col("station_lat").cast("double"))
    .withColumn("station_lng", col("station_lng").cast("double"))
    .select("station_code", "station_lat", "station_lng")
    .dropDuplicates(["station_code"])
    .filter(col("station_lat").isNotNull() & col("station_lng").isNotNull())
)

print("Loading weather data...")
# Read weather data and keep only relevant columns
# Use mergeSchema to handle potential schema differences across files
weather_raw = spark.read.option("mergeSchema", "true").parquet(WEATHER_PATH)

# Filter to only columns that exist in the data
available_cols = set(weather_raw.columns)
cols_to_select = [c for c in WEATHER_COLS_TO_KEEP if c in available_cols]
print(f"Selecting {len(cols_to_select)} weather columns: {cols_to_select}")

# Handle time column - convert from string or timestamp
weather = weather_raw.select(*cols_to_select)

# If time is a string, parse it; if timestamp with nanos, cast to timestamp
if "time" in weather.columns:
    time_type = str(weather.schema["time"].dataType)
    print(f"Time column type: {time_type}")
    if "String" in time_type:
        weather = weather.withColumn("weather_time", F.to_timestamp("time"))
    elif "Long" in time_type:
        # Long type - likely Unix timestamp in nanoseconds or seconds
        # Check magnitude to determine unit
        # Nanoseconds: ~1.7e18 for 2024, Milliseconds: ~1.7e12, Seconds: ~1.7e9
        # Convert from nanoseconds to seconds, then to timestamp
        weather = weather.withColumn(
            "weather_time",
            F.when(
                col("time") > 1e12, F.from_unixtime(col("time") / 1e9)
            )  # nanoseconds
            .when(
                col("time") > 1e10, F.from_unixtime(col("time") / 1e3)
            )  # milliseconds
            .otherwise(F.from_unixtime(col("time"))),  # seconds
        ).withColumn("weather_time", F.to_timestamp("weather_time"))
    else:
        # Cast to timestamp to handle other timestamp types
        weather = weather.withColumn("weather_time", col("time").cast("timestamp"))
else:
    raise ValueError("No 'time' column found in weather data")

weather = (
    weather.withColumn("weather_date", F.to_date("weather_time"))
    .withColumn("weather_hour", F.hour("weather_time"))
    .withColumn("weather_station", col("station"))
    .withColumn("weather_lat", col("lat").cast("double"))
    .withColumn("weather_lng", col("lon").cast("double"))
)

# Get unique weather stations with their coordinates
weather_stations = (
    weather.select("weather_station", "stationname", "weather_lat", "weather_lng")
    .dropDuplicates(["weather_station"])
    .filter(col("weather_lat").isNotNull() & col("weather_lng").isNotNull())
)

print(f"Found {stations.count()} train stations")
print(f"Found {weather_stations.count()} weather stations")


# === Step 1: Match train stations to nearest weather stations ===
print("Matching train stations to nearest weather stations...")

# Cross join to compute all pairwise distances
station_weather_pairs = stations.crossJoin(broadcast(weather_stations))

# Calculate squared Euclidean distance between each train station and weather station
station_weather_pairs = station_weather_pairs.withColumn(
    "distance_sq",
    euclidean_distance_squared(
        col("station_lat"), col("station_lng"), col("weather_lat"), col("weather_lng")
    ),
)

# For each train station, find the nearest weather station
window = Window.partitionBy("station_code").orderBy("distance_sq")

station_to_weather = (
    station_weather_pairs.withColumn("rank", F.row_number().over(window))
    .filter(col("rank") == 1)
    .select(
        "station_code",
        "station_lat",
        "station_lng",
        "weather_station",
        "stationname",
        "weather_lat",
        "weather_lng",
    )
    # Calculate actual distance in km using Haversine formula
    .withColumn(
        "distance_km",
        haversine_distance_km(
            col("station_lat"),
            col("station_lng"),
            col("weather_lat"),
            col("weather_lng"),
        ),
    )
)

print("Station to weather station mapping sample:")
station_to_weather.show(10, truncate=False)

# Cache the mapping for reuse
station_to_weather.cache()


# === Step 2: Load services and join with weather ===
print("Loading services data...")
services = spark.read.parquet(SERVICES_PATH)

# If services don't have stop_event_ts, compute it
if "stop_event_ts" not in services.columns:
    services = services.withColumn(
        "stop_event_ts", F.coalesce(col("stop_departure_ts"), col("stop_arrival_ts"))
    )

# filter out records without stop_event_ts

# number of records before filtering
initial_count = services.count()

print(f"Initial services record count: {initial_count}")

print("Filtering services without stop_event_ts...")

services = services.filter(col("stop_event_ts").isNotNull())
filtered_count = services.count()
print(
    f"Filtered services record count: {filtered_count} (removed {initial_count - filtered_count})"
)
# Extract date and hour for weather matching
services = services.withColumn(
    "service_weather_date", F.to_date("stop_event_ts")
).withColumn("service_weather_hour", F.hour("stop_event_ts"))

# Add weather station mapping to services based on stop_station_code
print("Joining services with station-to-weather mapping...")
station_mapping = station_to_weather.select(
    col("station_code").alias("mapped_station_code"),
    col("weather_station"),
    col("distance_km").alias("weather_station_distance_km"),
)
services_with_mapping = services.join(
    broadcast(station_mapping),
    services["stop_station_code"] == station_mapping["mapped_station_code"],
    "left",
).drop("mapped_station_code")

# Prepare weather data for hourly join
# Drop metadata columns, keep weather measurements
weather_measurement_cols = [
    c
    for c in cols_to_select
    if c not in ["station", "stationname", "lat", "lon", "time"]
]
weather_hourly = weather.select(
    col("weather_station").alias("w_station"),
    col("weather_date"),
    col("weather_hour"),
    *[col(c) for c in weather_measurement_cols if c in weather.columns],
)

# === Step 3: Join services with weather data ===
print("Joining services with weather data...")

# Join on weather station ID and time (date + hour)
services_with_weather = services_with_mapping.join(
    weather_hourly,
    (services_with_mapping["weather_station"] == weather_hourly["w_station"])
    & (services_with_mapping["service_weather_date"] == weather_hourly["weather_date"])
    & (services_with_mapping["service_weather_hour"] == weather_hourly["weather_hour"]),
    "left",
).drop("w_station")

# Drop temporary columns
services_with_weather = services_with_weather.drop(
    "service_weather_date", "service_weather_hour", "weather_date", "weather_hour"
)

print("Sample of services with weather data:")
services_with_weather.select(
    "stop_station_code",
    "weather_station",
    "weather_station_distance_km",
    "stop_event_ts",
    "T",
    "FF",
    "DR",
    "VV",
).show(100, truncate=False)

# === Diagnostics: Check unmatched records ===
total_count = services_with_weather.count()

# Check if ANY weather column has data (not just T)
weather_data_condition = (
    col("T").isNotNull()
    | col("FF").isNotNull()
    | col("DR").isNotNull()
    | col("VV").isNotNull()
    | col("P").isNotNull()
    | col("U").isNotNull()
    | col("DD").isNotNull()
    | col("FX").isNotNull()
    | col("N").isNotNull()
)
with_weather = services_with_weather.filter(weather_data_condition).count()
without_date = services_with_weather.filter(col("stop_event_ts").isNull()).count()
without_station_mapping = services_with_weather.filter(
    col("weather_station").isNull()
).count()

print(f"\n=== Merge Diagnostics ===")
print(f"Total service records: {total_count}")
print(
    f"Records with weather data: {with_weather} ({100*with_weather/total_count:.1f}%)"
)
print(
    f"Records without stop_event_ts (no date): {without_date} ({100*without_date/total_count:.1f}%)"
)
print(
    f"Records without weather station mapping: {without_station_mapping} ({100*without_station_mapping/total_count:.1f}%)"
)
print(
    f"Records with mapping but no weather: {total_count - with_weather - without_station_mapping - without_date}"
)

# Sample of services without a date
print("\nSample of services without stop_event_ts:")
services_with_weather.filter(col("stop_event_ts").isNull()).select(
    "service_rdt_id",
    "stop_station_code",
    "weather_station",
    "stop_arrival_time",
    "stop_departure_time",
).show(10, truncate=False)


# === Step 4: Write output ===
print(f"Writing output to {OUTPUT_PATH}...")
services_with_weather.write.mode("overwrite").partitionBy("year", "month").parquet(
    OUTPUT_PATH
)

# Also save the station mapping for reference
MAPPING_PATH = "final_project/data/master/station_weather_mapping"
print(f"Saving station-to-weather mapping to {MAPPING_PATH}...")
station_to_weather.write.mode("overwrite").parquet(MAPPING_PATH)

print("Done!")
spark.stop()
