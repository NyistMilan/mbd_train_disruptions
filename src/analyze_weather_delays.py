"""
Weather-Delay Correlation Analysis Script (PySpark Version)

This script analyzes the correlation between weather conditions and train delays
using the merged services_with_weather dataset on a Spark cluster.

Weather variables analyzed (from KNMI):
- T: Temperature (°C)
- T10N: Minimum temperature at 10cm (°C)
- TD: Dew point temperature (°C)
- U: Relative humidity (%)
- RH: Precipitation amount (mm)
- DR: Precipitation duration (hours)
- DD: Wind direction (degrees, 360=N, 90=E, 180=S, 270=W)
- FF: Past 10-min mean wind speed (m/s)
- FH: Hourly mean wind speed (m/s)
- FX: Maximum wind gust (m/s)
- VV: Visibility code (0-49: 100m steps, 50+: km ranges)
- N: Cloud cover (okta, 9=sky invisible)
- WW: Present weather code (WMO 4680)
- W1: Fog indicator (0/1)
- W2: Rain indicator (0/1)
- W3: Snow indicator (0/1)
- W5: Thunder indicator (0/1)
- W6: Ice formation indicator (0/1)
- P: Air pressure at sea level (hPa)
- SQ: Sunshine duration (hours)
- Q: Global solar radiation (J/cm²)

Delay metrics:
- stop_arrival_delay: Delay at arrival (minutes)
- stop_departure_delay: Delay at departure (minutes)
- service_maximum_delay: Maximum delay for the entire service

Usage:
    spark-submit --deploy-mode cluster --driver-memory 4g --executor-memory 4g \\
        src/analyze_weather_delays.py
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, when, count, mean, stddev, corr
from pyspark.sql.types import DoubleType
from datetime import datetime


# === Logging Class for HDFS Output ===
class HDFSLogger:
    """Logger that collects output and writes to HDFS as a text file."""

    def __init__(self):
        self.log_lines = []
        self.start_time = datetime.now()

    def log(self, message: str = ""):
        """Log a message (also prints to stdout)."""
        print(message)
        self.log_lines.append(message)

    def section(self, title: str):
        """Log a section header."""
        self.log("\n" + "=" * 70)
        self.log(title)
        self.log("=" * 70)

    def subsection(self, title: str):
        """Log a subsection header."""
        self.log("\n" + "-" * 70)
        self.log(title)
        self.log("-" * 70)

    def save_to_hdfs(self, spark, output_path: str):
        """Save the log to HDFS as a text file."""
        # Add timing info
        end_time = datetime.now()
        duration = end_time - self.start_time

        self.log("\n" + "=" * 70)
        self.log("JOB TIMING")
        self.log("=" * 70)
        self.log(f"Start time: {self.start_time}")
        self.log(f"End time: {end_time}")
        self.log(f"Duration: {duration}")

        # Write to HDFS
        log_content = "\n".join(self.log_lines)
        log_path = f"{output_path}/analysis_log.txt"

        # Create RDD from log content and save as text file
        log_rdd = spark.sparkContext.parallelize([log_content], 1)

        # Delete existing log if present
        try:
            hadoop_conf = spark._jsc.hadoopConfiguration()
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
            path = spark._jvm.org.apache.hadoop.fs.Path(log_path)
            if fs.exists(path):
                fs.delete(path, True)
        except Exception as e:
            print(f"Note: Could not delete existing log: {e}")

        log_rdd.saveAsTextFile(log_path)
        print(f"\nLog saved to: {log_path}")
        print("To view the log, run:")
        print(f"  hdfs dfs -cat {log_path}/part-00000")


# Initialize logger
logger = HDFSLogger()


# === Configuration ===
DATA_PATH = "/user/s3544648/final_project/data/master/services_with_weather"
OUTPUT_PATH = "/user/s3544648/final_project/data/analysis"

# All weather columns from KNMI dataset
WEATHER_COLS = [
    # Temperature
    "T",  # Air temperature (°C)
    "T10N",  # Minimum temperature at 10cm (°C)
    "TD",  # Dew point temperature (°C)
    # Humidity
    "U",  # Relative humidity (%)
    # Precipitation
    "RH",  # Precipitation amount (mm)
    "DR",  # Precipitation duration (hours)
    # Wind
    "DD",  # Wind direction (degrees)
    "FF",  # Past 10-min mean wind speed (m/s)
    "FH",  # Hourly mean wind speed (m/s)
    "FX",  # Maximum wind gust (m/s)
    # Visibility & clouds
    "VV",  # Visibility code
    "N",  # Cloud cover (okta)
    # Weather phenomena
    "WW",  # Present weather code
    "W1",  # Fog indicator (0/1)
    "W2",  # Rain indicator (0/1)
    "W3",  # Snow indicator (0/1)
    "W5",  # Thunder indicator (0/1)
    "W6",  # Ice formation indicator (0/1)
    # Pressure
    "P",  # Air pressure at sea level (hPa)
    # Sunshine
    "SQ",  # Sunshine duration (hours)
    "Q",  # Global solar radiation (J/cm²)
]

# Human-readable names for weather variables
WEATHER_LABELS = {
    "T": "Temperature (°C)",
    "T10N": "Min Temp at 10cm (°C)",
    "TD": "Dew Point (°C)",
    "U": "Relative Humidity (%)",
    "RH": "Precipitation Amount (mm)",
    "DR": "Precipitation Duration (hr)",
    "DD": "Wind Direction (degrees)",
    "FF": "Mean Wind Speed 10min (m/s)",
    "FH": "Hourly Mean Wind Speed (m/s)",
    "FX": "Max Wind Gust (m/s)",
    "VV": "Visibility (code)",
    "N": "Cloud Cover (okta)",
    "WW": "Weather Code (WMO)",
    "W1": "Fog Indicator",
    "W2": "Rain Indicator",
    "W3": "Snow Indicator",
    "W5": "Thunder Indicator",
    "W6": "Ice Indicator",
    "P": "Air Pressure (hPa)",
    "SQ": "Sunshine Duration (hr)",
    "Q": "Solar Radiation (J/cm²)",
}

# Delay columns
DELAY_COLS = ["stop_arrival_delay", "stop_departure_delay", "service_maximum_delay"]

# Initialize Spark
spark = (
    SparkSession.builder.appName("weather_delay_correlation_analysis")
    .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


def load_data(data_path: str):
    """Load the services_with_weather parquet data."""
    logger.section("LOADING DATA")
    logger.log(f"Loading data from {data_path}...")

    df = spark.read.parquet(data_path)

    total_count = df.count()
    logger.log(f"Loaded {total_count:,} records")
    logger.log(f"Columns: {df.columns}")

    return df


def preprocess_data(df):
    """Clean and preprocess the data for analysis."""
    logger.section("PREPROCESSING DATA")

    # Weather data is already in correct units from KNMI:
    # T, TD: °C | FF, FH, FX: m/s | DR: hours | P: hPa

    # Only convert DR from hours to minutes for easier interpretation
    if "DR" in df.columns:
        df = df.withColumn("DR_minutes", col("DR").cast(DoubleType()) * 60)

    # Create binary delay indicators (basic thresholds only)
    for delay_col in DELAY_COLS:
        if delay_col in df.columns:
            df = df.withColumn(
                f"{delay_col}_binary", when(col(delay_col) > 0, 1).otherwise(0)
            )
            df = df.withColumn(
                f"{delay_col}_significant", when(col(delay_col) > 5, 1).otherwise(0)
            )

    # Create cancellation indicator if available
    if "stop_arrival_cancelled" in df.columns:
        df = df.withColumn(
            "is_cancelled", when(col("stop_arrival_cancelled") == True, 1).otherwise(0)
        )
    elif "service_completely_cancelled" in df.columns:
        df = df.withColumn(
            "is_cancelled",
            when(col("service_completely_cancelled") == True, 1).otherwise(0),
        )

    # Create weather categories
    if "T" in df.columns:
        df = df.withColumn(
            "temp_category",
            when(col("T") < 0, "Freezing (<0°C)")
            .when(col("T") < 10, "Cold (0-10°C)")
            .when(col("T") < 20, "Mild (10-20°C)")
            .otherwise("Warm (>20°C)"),
        )

    if "FF" in df.columns:
        df = df.withColumn(
            "wind_category",
            when(col("FF") < 5, "Calm (<5 m/s)")
            .when(col("FF") < 10, "Light (5-10 m/s)")
            .when(col("FF") < 15, "Moderate (10-15 m/s)")
            .otherwise("Strong (>15 m/s)"),
        )

    if "DR_minutes" in df.columns:
        df = df.withColumn(
            "rain_category",
            when(col("DR_minutes") == 0, "No Rain")
            .when(col("DR_minutes") < 10, "Light Rain")
            .when(col("DR_minutes") < 30, "Moderate Rain")
            .otherwise("Heavy Rain"),
        )

    logger.log("Preprocessing complete.")
    return df


def calculate_correlations(df):
    """Calculate correlations between weather variables and delays using Spark."""
    logger.section("CALCULATING CORRELATIONS")

    # Get available columns
    available_weather = [c for c in WEATHER_COLS if c in df.columns]
    available_delay = [c for c in DELAY_COLS if c in df.columns]

    logger.log(f"Weather columns: {available_weather}")
    logger.log(f"Delay columns: {available_delay}")

    results = []

    for weather_col in available_weather:
        for delay_col in available_delay:
            # Filter out nulls for this pair
            pair_df = df.select(weather_col, delay_col).dropna()
            n_samples = pair_df.count()

            if n_samples < 100:
                continue

            # Calculate Pearson correlation using Spark's built-in function
            correlation = pair_df.select(
                corr(
                    col(weather_col).cast(DoubleType()),
                    col(delay_col).cast(DoubleType()),
                )
            ).collect()[0][0]

            if correlation is not None:
                results.append(
                    {
                        "weather_variable": weather_col,
                        "weather_label": WEATHER_LABELS.get(weather_col, weather_col),
                        "delay_variable": delay_col,
                        "n_samples": n_samples,
                        "correlation": correlation,
                        "abs_correlation": abs(correlation),
                    }
                )

    # Create DataFrame and sort by absolute correlation
    if results:
        corr_df = spark.createDataFrame(results)
        corr_df = corr_df.orderBy(col("abs_correlation").desc())
        return corr_df

    return None


def calculate_delay_stats_by_category(df):
    """Calculate delay statistics for different weather conditions."""
    logger.section("CALCULATING DELAY RATES BY WEATHER CONDITION")

    results = {}

    # Stats by temperature category
    if "temp_category" in df.columns and "stop_arrival_delay" in df.columns:
        temp_stats = (
            df.groupBy("temp_category")
            .agg(
                count("*").alias("count"),
                mean("stop_arrival_delay").alias("mean_delay"),
                stddev("stop_arrival_delay").alias("std_delay"),
                mean("stop_arrival_delay_binary").alias("delay_rate"),
                mean("stop_arrival_delay_significant").alias("significant_delay_rate"),
            )
            .orderBy("temp_category")
        )
        results["temperature"] = temp_stats
        logger.log("\nCalculated delay stats by Temperature Category")

    # Stats by wind category
    if "wind_category" in df.columns and "stop_arrival_delay" in df.columns:
        wind_stats = (
            df.groupBy("wind_category")
            .agg(
                count("*").alias("count"),
                mean("stop_arrival_delay").alias("mean_delay"),
                stddev("stop_arrival_delay").alias("std_delay"),
                mean("stop_arrival_delay_binary").alias("delay_rate"),
                mean("stop_arrival_delay_significant").alias("significant_delay_rate"),
            )
            .orderBy("wind_category")
        )
        results["wind"] = wind_stats
        logger.log("Calculated delay stats by Wind Category")

    # Stats by precipitation category
    if "rain_category" in df.columns and "stop_arrival_delay" in df.columns:
        rain_stats = (
            df.groupBy("rain_category")
            .agg(
                count("*").alias("count"),
                mean("stop_arrival_delay").alias("mean_delay"),
                stddev("stop_arrival_delay").alias("std_delay"),
                mean("stop_arrival_delay_binary").alias("delay_rate"),
                mean("stop_arrival_delay_significant").alias("significant_delay_rate"),
            )
            .orderBy("rain_category")
        )
        results["precipitation"] = rain_stats
        logger.log("Calculated delay stats by Precipitation Category")

    return results


def analyze_extreme_weather(df):
    """Analyze delay patterns during extreme weather conditions."""
    logger.section("EXTREME WEATHER ANALYSIS")

    results = []

    # Extreme cold (< 0°C) vs Normal (5-20°C)
    if "T" in df.columns:
        freezing = df.filter(col("T") < 0)
        normal_temp = df.filter((col("T") >= 5) & (col("T") <= 20))

        freezing_stats = freezing.agg(
            mean("stop_arrival_delay").alias("mean_delay"),
            mean("stop_arrival_delay_binary").alias("delay_rate"),
            count("*").alias("count"),
        ).collect()[0]

        normal_temp_stats = normal_temp.agg(
            mean("stop_arrival_delay").alias("mean_delay"),
            mean("stop_arrival_delay_binary").alias("delay_rate"),
            count("*").alias("count"),
        ).collect()[0]

        if freezing_stats["count"] > 0:
            results.append(
                {
                    "condition": "Freezing (<0°C)",
                    "mean_delay": (
                        float(freezing_stats["mean_delay"])
                        if freezing_stats["mean_delay"]
                        else 0.0
                    ),
                    "delay_rate_pct": (
                        float(freezing_stats["delay_rate"]) * 100
                        if freezing_stats["delay_rate"]
                        else 0.0
                    ),
                    "n_records": int(freezing_stats["count"]),
                }
            )
        if normal_temp_stats["count"] > 0:
            results.append(
                {
                    "condition": "Normal Temp (5-20°C)",
                    "mean_delay": (
                        float(normal_temp_stats["mean_delay"])
                        if normal_temp_stats["mean_delay"]
                        else 0.0
                    ),
                    "delay_rate_pct": (
                        float(normal_temp_stats["delay_rate"]) * 100
                        if normal_temp_stats["delay_rate"]
                        else 0.0
                    ),
                    "n_records": int(normal_temp_stats["count"]),
                }
            )

    # High wind (> 15 m/s) vs Calm (< 5 m/s)
    if "FF" in df.columns:
        high_wind = df.filter(col("FF") > 15)
        calm = df.filter(col("FF") < 5)

        high_wind_stats = high_wind.agg(
            mean("stop_arrival_delay").alias("mean_delay"),
            mean("stop_arrival_delay_binary").alias("delay_rate"),
            count("*").alias("count"),
        ).collect()[0]

        calm_stats = calm.agg(
            mean("stop_arrival_delay").alias("mean_delay"),
            mean("stop_arrival_delay_binary").alias("delay_rate"),
            count("*").alias("count"),
        ).collect()[0]

        if high_wind_stats["count"] > 0:
            results.append(
                {
                    "condition": "Strong Wind (>15 m/s)",
                    "mean_delay": (
                        float(high_wind_stats["mean_delay"])
                        if high_wind_stats["mean_delay"]
                        else 0.0
                    ),
                    "delay_rate_pct": (
                        float(high_wind_stats["delay_rate"]) * 100
                        if high_wind_stats["delay_rate"]
                        else 0.0
                    ),
                    "n_records": int(high_wind_stats["count"]),
                }
            )
        if calm_stats["count"] > 0:
            results.append(
                {
                    "condition": "Calm (<5 m/s)",
                    "mean_delay": (
                        float(calm_stats["mean_delay"])
                        if calm_stats["mean_delay"]
                        else 0.0
                    ),
                    "delay_rate_pct": (
                        float(calm_stats["delay_rate"]) * 100
                        if calm_stats["delay_rate"]
                        else 0.0
                    ),
                    "n_records": int(calm_stats["count"]),
                }
            )

    # Heavy rain vs No rain
    if "DR_minutes" in df.columns:
        heavy_rain = df.filter(col("DR_minutes") > 30)
        no_rain = df.filter(col("DR_minutes") == 0)

        heavy_rain_stats = heavy_rain.agg(
            mean("stop_arrival_delay").alias("mean_delay"),
            mean("stop_arrival_delay_binary").alias("delay_rate"),
            count("*").alias("count"),
        ).collect()[0]

        no_rain_stats = no_rain.agg(
            mean("stop_arrival_delay").alias("mean_delay"),
            mean("stop_arrival_delay_binary").alias("delay_rate"),
            count("*").alias("count"),
        ).collect()[0]

        if heavy_rain_stats["count"] > 0:
            results.append(
                {
                    "condition": "Heavy Rain (>30 min/hr)",
                    "mean_delay": (
                        float(heavy_rain_stats["mean_delay"])
                        if heavy_rain_stats["mean_delay"]
                        else 0.0
                    ),
                    "delay_rate_pct": (
                        float(heavy_rain_stats["delay_rate"]) * 100
                        if heavy_rain_stats["delay_rate"]
                        else 0.0
                    ),
                    "n_records": int(heavy_rain_stats["count"]),
                }
            )
        if no_rain_stats["count"] > 0:
            results.append(
                {
                    "condition": "No Rain",
                    "mean_delay": (
                        float(no_rain_stats["mean_delay"])
                        if no_rain_stats["mean_delay"]
                        else 0.0
                    ),
                    "delay_rate_pct": (
                        float(no_rain_stats["delay_rate"]) * 100
                        if no_rain_stats["delay_rate"]
                        else 0.0
                    ),
                    "n_records": int(no_rain_stats["count"]),
                }
            )

    if results:
        extreme_df = spark.createDataFrame(results)
        logger.log(f"\nExtreme Weather Impact: {len(results)} conditions analyzed")
        return extreme_df

    return None


def generate_aggregated_data_for_plots(df):
    """Generate aggregated data that can be used for plotting locally."""
    logger.section("GENERATING AGGREGATED DATA FOR VISUALIZATION")

    aggregations = {}

    # 1. Hourly aggregation by weather bins for scatter-like visualization
    if "T" in df.columns:
        temp_bins = (
            df.withColumn(
                "temp_bin", (F.floor(col("T") / 2) * 2).cast("int")  # 2°C bins
            )
            .groupBy("temp_bin")
            .agg(
                mean("stop_arrival_delay").alias("mean_delay"),
                stddev("stop_arrival_delay").alias("std_delay"),
                count("*").alias("count"),
            )
            .orderBy("temp_bin")
        )
        aggregations["temp_bins"] = temp_bins

    if "FF" in df.columns:
        wind_bins = (
            df.withColumn("wind_bin", (F.floor(col("FF"))).cast("int"))  # 1 m/s bins
            .groupBy("wind_bin")
            .agg(
                mean("stop_arrival_delay").alias("mean_delay"),
                stddev("stop_arrival_delay").alias("std_delay"),
                count("*").alias("count"),
            )
            .orderBy("wind_bin")
        )
        aggregations["wind_bins"] = wind_bins

    if "DR_minutes" in df.columns:
        rain_bins = (
            df.withColumn(
                "rain_bin",
                (F.floor(col("DR_minutes") / 5) * 5).cast("int"),  # 5 min bins
            )
            .groupBy("rain_bin")
            .agg(
                mean("stop_arrival_delay").alias("mean_delay"),
                stddev("stop_arrival_delay").alias("std_delay"),
                count("*").alias("count"),
            )
            .orderBy("rain_bin")
        )
        aggregations["rain_bins"] = rain_bins

    if "VV" in df.columns:
        visibility_bins = (
            df.withColumn(
                "visibility_bin",
                (F.floor(col("VV") / 10) * 10).cast("int"),  # 10-unit bins
            )
            .groupBy("visibility_bin")
            .agg(
                mean("stop_arrival_delay").alias("mean_delay"),
                stddev("stop_arrival_delay").alias("std_delay"),
                count("*").alias("count"),
            )
            .orderBy("visibility_bin")
        )
        aggregations["visibility_bins"] = visibility_bins

    # Q solar radiation
    if "Q" in df.columns:
        solar_bins = (
            df.withColumn(
                "solar_bin",
                (F.floor(col("Q") / 100) * 100).cast("int"),  # 100 J/cm² bins
            )
            .groupBy("solar_bin")
            .agg(
                mean("stop_arrival_delay").alias("mean_delay"),
                stddev("stop_arrival_delay").alias("std_delay"),
                count("*").alias("count"),
            )
            .orderBy("solar_bin")
        )
        aggregations["solar_bins"] = solar_bins

    # SQ sunshine duration
    if "SQ" in df.columns:
        sunshine_bins = (
            df.withColumn(
                "sunshine_bin",
                (F.floor(col("SQ"))).cast("int"),  # 1 hour bins
            )
            .groupBy("sunshine_bin")
            .agg(
                mean("stop_arrival_delay").alias("mean_delay"),
                stddev("stop_arrival_delay").alias("std_delay"),
                count("*").alias("count"),
            )
            .orderBy("sunshine_bin")
        )
        aggregations["sunshine_bins"] = sunshine_bins

    # P air pressure
    if "P" in df.columns:
        pressure_bins = (
            df.withColumn(
                "pressure_bin",
                (F.floor(col("P") / 5) * 5).cast("int"),  # 5 hPa bins
            )
            .groupBy("pressure_bin")
            .agg(
                mean("stop_arrival_delay").alias("mean_delay"),
                stddev("stop_arrival_delay").alias("std_delay"),
                count("*").alias("count"),
            )
            .orderBy("pressure_bin")
        )
        aggregations["pressure_bins"] = pressure_bins

    # W2 Rain indicator
    if "W2" in df.columns:
        rain_indicator = (
            df.groupBy("W2")
            .agg(
                mean("stop_arrival_delay").alias("mean_delay"),
                stddev("stop_arrival_delay").alias("std_delay"),
                count("*").alias("count"),
            )
            .orderBy("W2")
        )
        aggregations["rain_indicator"] = rain_indicator

    # U Relative Humidity

    if "U" in df.columns:
        humidity_bins = (
            df.withColumn(
                "humidity_bin",
                (F.floor(col("U") / 10) * 10).cast("int"),  # 10% bins
            )
            .groupBy("humidity_bin")
            .agg(
                mean("stop_arrival_delay").alias("mean_delay"),
                stddev("stop_arrival_delay").alias("std_delay"),
                count("*").alias("count"),
            )
            .orderBy("humidity_bin")
        )
        aggregations["humidity_bins"] = humidity_bins

    return aggregations


def save_results(corr_df, delay_stats, extreme_df, aggregations, output_path):
    """Save all analysis results to HDFS."""
    logger.section("SAVING RESULTS")

    # Save correlations
    if corr_df is not None:
        corr_path = f"{output_path}/correlations"
        corr_df.coalesce(1).write.mode("overwrite").option("header", True).csv(
            corr_path
        )
        logger.log(f"Saved correlations to: {corr_path}")

    # Save delay stats by category
    for name, stats_df in delay_stats.items():
        stats_path = f"{output_path}/delay_stats_{name}"
        stats_df.coalesce(1).write.mode("overwrite").option("header", True).csv(
            stats_path
        )
        logger.log(f"Saved {name} stats to: {stats_path}")

    # Save extreme weather analysis
    if extreme_df is not None:
        extreme_path = f"{output_path}/extreme_weather"
        extreme_df.coalesce(1).write.mode("overwrite").option("header", True).csv(
            extreme_path
        )
        logger.log(f"Saved extreme weather analysis to: {extreme_path}")

    # Save aggregated data for plotting
    for name, agg_df in aggregations.items():
        agg_path = f"{output_path}/aggregated_{name}"
        agg_df.coalesce(1).write.mode("overwrite").option("header", True).csv(agg_path)
        logger.log(f"Saved aggregated {name} to: {agg_path}")


def print_summary_report(corr_df, delay_stats, extreme_df, total_count):
    """Print a summary report of the analysis."""
    logger.section("WEATHER-DELAY CORRELATION ANALYSIS REPORT")
    logger.log(f"\nDataset: {total_count:,} train service records with weather data")

    logger.subsection("1. CORRELATION SUMMARY")
    logger.log("\nTop correlations between weather variables and arrival delays:")

    if corr_df is not None:
        arrival_corr = (
            corr_df.filter(col("delay_variable") == "stop_arrival_delay")
            .orderBy(col("abs_correlation").desc())
            .limit(10)
        )

        rows = arrival_corr.collect()
        for row in rows:
            logger.log(f"  {row['weather_label']:40} r={row['correlation']:+.4f}")

    logger.log("\nINTERPRETATION:")
    logger.log("  - Positive correlation: Higher values → more delays")
    logger.log("  - Negative correlation: Higher values → fewer delays")
    logger.log("  - |r| < 0.1: Very weak correlation")
    logger.log("  - 0.1 ≤ |r| < 0.3: Weak correlation")
    logger.log("  - 0.3 ≤ |r| < 0.5: Moderate correlation")
    logger.log("  - |r| ≥ 0.5: Strong correlation")

    logger.subsection("2. KEY FINDINGS")

    if corr_df is not None:
        # Get mean absolute correlation
        mean_corr = (
            corr_df.filter(col("delay_variable") == "stop_arrival_delay")
            .agg(mean("abs_correlation"))
            .collect()[0][0]
        )

        if mean_corr:
            logger.log(f"\n  • Average absolute correlation: {mean_corr:.4f}")

            if mean_corr < 0.1:
                strength = "very weak"
            elif mean_corr < 0.3:
                strength = "weak"
            else:
                strength = "moderate"

            logger.log(f"\n  CONCLUSION:")
            logger.log(
                f"    Weather conditions show a {strength} correlation with train delays."
            )
            logger.log(
                "    Other factors (infrastructure, scheduling, incidents) likely play"
            )
            logger.log("    a more significant role in causing delays.")


# === Main Analysis Pipeline ===
if __name__ == "__main__":
    logger.section("WEATHER-DELAY CORRELATION ANALYSIS (PySpark)")
    logger.log("Script: analyze_weather_delays.py (Basic Correlation Analysis)")

    # Load data
    df = load_data(DATA_PATH)
    total_count = df.count()

    # Cache the dataframe for repeated use
    df = df.cache()

    # Preprocess
    df = preprocess_data(df)

    # Calculate correlations
    corr_df = calculate_correlations(df)
    if corr_df is not None:
        logger.log("\nCorrelation Results (top 20):")
        corr_rows = corr_df.limit(20).collect()
        for row in corr_rows:
            logger.log(
                f"  {row['weather_label']:40} vs {row['delay_variable']:25} "
                f"r={row['correlation']:+.4f}"
            )

    # Calculate delay stats by category
    delay_stats = calculate_delay_stats_by_category(df)

    # Extreme weather analysis
    extreme_df = analyze_extreme_weather(df)

    # Generate aggregated data for visualization
    aggregations = generate_aggregated_data_for_plots(df)

    # Save all results
    save_results(corr_df, delay_stats, extreme_df, aggregations, OUTPUT_PATH)

    # Print summary report
    print_summary_report(corr_df, delay_stats, extreme_df, total_count)

    logger.section("ANALYSIS COMPLETE")
    logger.log(f"\nAll results saved to: {OUTPUT_PATH}")
    logger.log("\nFor extended analysis with different delay thresholds, run:")
    logger.log("  spark-submit src/analyze_weather_delays_extended.py")
    logger.log("\nTo create visualizations, download the CSV files and run:")
    logger.log("  python src/plot_weather_analysis.py")

    # Save log to HDFS
    logger.save_to_hdfs(spark, OUTPUT_PATH)

    # Unpersist cached data
    df.unpersist()

    spark.stop()
