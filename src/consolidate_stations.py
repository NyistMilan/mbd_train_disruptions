"""
Consolidate all historical train station data into a single master file.

This script reads station files from multiple years and creates a unified
stations dataset with all unique stations and their properties.
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col

# === Configuration ===
RAW_BASE = "/user/s3544648/final_project/data/raw"
STATIONS_INPUT_PATH = f"{RAW_BASE}/stations/*"
STATIONS_OUTPUT_PATH = f"{RAW_BASE}/stations_consolidated"

spark = (
    SparkSession.builder
    .appName("consolidate_stations")
    .getOrCreate()
)

# === Load all station files ===
print(f"Loading station data from: {STATIONS_INPUT_PATH}")
stations_raw = spark.read.option("header", True).csv(STATIONS_INPUT_PATH)

print(f"Total rows loaded: {stations_raw.count()}")
print(f"Columns: {stations_raw.columns}")

# Show sample of raw data
print("\nSample of raw station data:")
stations_raw.show(5, truncate=False)

# === Clean and deduplicate ===
# Standardize the station code (uppercase, trimmed)
stations_cleaned = (
    stations_raw
    .withColumn("code", F.upper(F.trim(col("code"))))
    .withColumn("geo_lat", col("geo_lat").cast("double"))
    .withColumn("geo_lng", col("geo_lng").cast("double"))
)

# Count unique stations before deduplication
unique_codes = stations_cleaned.select("code").distinct().count()
print(f"\nUnique station codes found: {unique_codes}")

# For stations that appear in multiple files, keep the most recent/complete record
# Strategy: prefer records with non-null coordinates, then take any
stations_with_coords = stations_cleaned.filter(
    col("geo_lat").isNotNull() & col("geo_lng").isNotNull()
)
stations_without_coords = stations_cleaned.filter(
    col("geo_lat").isNull() | col("geo_lng").isNull()
)

print(f"Stations with coordinates: {stations_with_coords.select('code').distinct().count()}")
print(f"Stations without coordinates: {stations_without_coords.select('code').distinct().count()}")

# Deduplicate: prefer records with coordinates
# First, get unique stations with coordinates
stations_consolidated = stations_with_coords.dropDuplicates(["code"])

# Then add stations that only exist without coordinates (if any)
codes_with_coords = stations_consolidated.select("code")
stations_only_without_coords = (
    stations_without_coords
    .join(codes_with_coords, "code", "left_anti")  # Keep only codes not in coords set
    .dropDuplicates(["code"])
)

# Union both sets
stations_final = stations_consolidated.union(stations_only_without_coords)

print(f"\nFinal consolidated stations: {stations_final.count()}")

# === Show statistics ===
print("\n=== Station Statistics ===")
print(f"Total unique stations: {stations_final.count()}")

# Count by country if available
if "country" in stations_final.columns:
    print("\nStations by country:")
    stations_final.groupBy("country").count().orderBy(col("count").desc()).show(20)

# Check for missing coordinates
missing_coords = stations_final.filter(
    col("geo_lat").isNull() | col("geo_lng").isNull()
).count()
print(f"\nStations missing coordinates: {missing_coords}")

# Show sample
print("\nSample of consolidated stations:")
stations_final.show(20, truncate=False)

# === Save output ===
print(f"\nSaving consolidated stations to: {STATIONS_OUTPUT_PATH}")

# Save as both Parquet and CSV for flexibility
stations_final.write.mode("overwrite").parquet(STATIONS_OUTPUT_PATH)

# Also save as single CSV for easy inspection
stations_final.coalesce(1).write.mode("overwrite").option("header", True).csv(
    f"{STATIONS_OUTPUT_PATH}_csv"
)

print("Done!")
spark.stop()
