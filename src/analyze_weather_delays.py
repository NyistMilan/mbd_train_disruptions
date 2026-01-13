"""
Weather-Delay Correlation Analysis Script (PySpark Version)

This script analyzes the correlation between weather conditions and train delays
using the merged services_with_weather dataset on a Spark cluster.

Weather variables analyzed:
- T: Temperature (0.1 °C)
- FF/FH: Wind speed (0.1 m/s)
- FX: Maximum wind gust (0.1 m/s)
- DR: Precipitation duration (0.1 hour)
- VV: Visibility
- N: Cloud cover (octants)
- P: Air pressure (0.1 hPa)
- U: Relative humidity (%)
- DD: Wind direction (degrees)

Delay metrics:
- stop_arrival_delay: Delay at arrival (minutes)
- stop_departure_delay: Delay at departure (minutes)
- service_maximum_delay: Maximum delay for the entire service

Usage:
    spark-submit --deploy-mode cluster --driver-memory 4g --executor-memory 4g \
        src/analyze_weather_delays.py
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, when, lit, count, mean, stddev, corr
from pyspark.sql.types import DoubleType
from datetime import datetime
import io


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

# Weather columns to analyze (raw KNMI values)
WEATHER_COLS = ["T", "FF", "FH", "FX", "DR", "VV", "N", "P", "U", "DD", "TD", "SQ"]

# Human-readable names for weather variables
WEATHER_LABELS = {
    "T": "Temperature (0.1°C)",
    "FF": "Mean Wind Speed (0.1 m/s)",
    "FH": "Hourly Wind Speed (0.1 m/s)",
    "FX": "Max Wind Gust (0.1 m/s)",
    "DR": "Precipitation Duration (0.1 hr)",
    "VV": "Visibility (code/km)",
    "N": "Cloud Cover (octants)",
    "P": "Air Pressure (0.1 hPa)",
    "U": "Relative Humidity (%)",
    "DD": "Wind Direction (degrees)",
    "TD": "Dew Point (0.1°C)",
    "SQ": "Sunshine Duration (0.1 hr)",
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
    print(f"\n{'='*70}")
    print("PREPROCESSING DATA")
    print(f"{'='*70}")

    # Convert weather values to real units
    # Temperature: 0.1°C -> °C
    if "T" in df.columns:
        df = df.withColumn("T_celsius", col("T").cast(DoubleType()) / 10.0)
    if "TD" in df.columns:
        df = df.withColumn("TD_celsius", col("TD").cast(DoubleType()) / 10.0)

    # Wind speed: 0.1 m/s -> m/s
    for c in ["FF", "FH", "FX"]:
        if c in df.columns:
            df = df.withColumn(f"{c}_ms", col(c).cast(DoubleType()) / 10.0)

    # Precipitation duration: 0.1 hr -> minutes
    if "DR" in df.columns:
        df = df.withColumn("DR_minutes", col("DR").cast(DoubleType()) * 6)

    # Create binary delay indicators with multiple thresholds
    for delay_col in DELAY_COLS:
        if delay_col in df.columns:
            df = df.withColumn(
                f"{delay_col}_binary", when(col(delay_col) > 0, 1).otherwise(0)
            )
            df = df.withColumn(
                f"{delay_col}_significant", when(col(delay_col) > 5, 1).otherwise(0)
            )
            # New: severe delays (>15 min)
            df = df.withColumn(
                f"{delay_col}_severe", when(col(delay_col) > 15, 1).otherwise(0)
            )
            # New: very severe delays (>30 min)
            df = df.withColumn(
                f"{delay_col}_very_severe", when(col(delay_col) > 30, 1).otherwise(0)
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
    if "T_celsius" in df.columns:
        df = df.withColumn(
            "temp_category",
            when(col("T_celsius") < 0, "Freezing (<0°C)")
            .when(col("T_celsius") < 10, "Cold (0-10°C)")
            .when(col("T_celsius") < 20, "Mild (10-20°C)")
            .otherwise("Warm (>20°C)"),
        )

    if "FF_ms" in df.columns:
        df = df.withColumn(
            "wind_category",
            when(col("FF_ms") < 5, "Calm (<5 m/s)")
            .when(col("FF_ms") < 10, "Light (5-10 m/s)")
            .when(col("FF_ms") < 15, "Moderate (10-15 m/s)")
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
    if "T_celsius" in df.columns:
        freezing = df.filter(col("T_celsius") < 0)
        normal_temp = df.filter((col("T_celsius") >= 5) & (col("T_celsius") <= 20))

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
    if "FF_ms" in df.columns:
        high_wind = df.filter(col("FF_ms") > 15)
        calm = df.filter(col("FF_ms") < 5)

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
    if "T_celsius" in df.columns:
        temp_bins = (
            df.withColumn(
                "temp_bin", (F.floor(col("T_celsius") / 2) * 2).cast("int")  # 2°C bins
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

    if "FF_ms" in df.columns:
        wind_bins = (
            df.withColumn("wind_bin", (F.floor(col("FF_ms"))).cast("int"))  # 1 m/s bins
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

    return aggregations


def calculate_correlations_by_delay_metric(df):
    """Calculate correlations for different delay severity levels."""
    logger.section("CORRELATIONS BY DELAY METRIC")

    available_weather = [c for c in WEATHER_COLS if c in df.columns]

    # Different delay metrics to analyze
    delay_metrics = {
        "any_delay": "stop_arrival_delay_binary",
        "significant_delay_gt5min": "stop_arrival_delay_significant",
        "severe_delay_gt15min": "stop_arrival_delay_severe",
        "very_severe_delay_gt30min": "stop_arrival_delay_very_severe",
    }

    # Add cancellation if available
    if "is_cancelled" in df.columns:
        delay_metrics["cancellation"] = "is_cancelled"

    results = []

    for metric_name, metric_col in delay_metrics.items():
        if metric_col not in df.columns:
            continue

        for weather_col in available_weather:
            pair_df = df.select(weather_col, metric_col).dropna()
            n_samples = pair_df.count()

            if n_samples < 100:
                continue

            correlation = pair_df.select(
                corr(
                    col(weather_col).cast(DoubleType()),
                    col(metric_col).cast(DoubleType()),
                )
            ).collect()[0][0]

            if correlation is not None:
                results.append(
                    {
                        "delay_metric": metric_name,
                        "weather_variable": weather_col,
                        "weather_label": WEATHER_LABELS.get(weather_col, weather_col),
                        "n_samples": n_samples,
                        "correlation": correlation,
                        "abs_correlation": abs(correlation),
                    }
                )

    if results:
        corr_df = spark.createDataFrame(results)
        corr_df = corr_df.orderBy(col("delay_metric"), col("abs_correlation").desc())
        logger.log(f"\nCorrelations by Delay Metric: {len(results)} correlations calculated")
        return corr_df

    return None


def analyze_true_extreme_events(df):
    """Analyze delay patterns during truly extreme weather events."""
    logger.section("TRUE EXTREME WEATHER EVENTS ANALYSIS")

    results = []

    # Calculate baseline statistics first
    baseline_stats = df.agg(
        mean("stop_arrival_delay").alias("mean_delay"),
        mean("stop_arrival_delay_binary").alias("delay_rate"),
        mean("stop_arrival_delay_significant").alias("sig_delay_rate"),
        mean("stop_arrival_delay_severe").alias("severe_delay_rate"),
        count("*").alias("count"),
    ).collect()[0]

    baseline = {
        "condition": "BASELINE (All Data)",
        "mean_delay": (
            float(baseline_stats["mean_delay"]) if baseline_stats["mean_delay"] else 0.0
        ),
        "delay_rate_pct": (
            float(baseline_stats["delay_rate"]) * 100
            if baseline_stats["delay_rate"]
            else 0.0
        ),
        "sig_delay_rate_pct": (
            float(baseline_stats["sig_delay_rate"]) * 100
            if baseline_stats["sig_delay_rate"]
            else 0.0
        ),
        "severe_delay_rate_pct": (
            float(baseline_stats["severe_delay_rate"]) * 100
            if baseline_stats["severe_delay_rate"]
            else 0.0
        ),
        "n_records": int(baseline_stats["count"]),
    }
    results.append(baseline)

    def get_stats(filtered_df, condition_name):
        """Helper to get stats for a filtered dataframe."""
        stats = filtered_df.agg(
            mean("stop_arrival_delay").alias("mean_delay"),
            mean("stop_arrival_delay_binary").alias("delay_rate"),
            mean("stop_arrival_delay_significant").alias("sig_delay_rate"),
            mean("stop_arrival_delay_severe").alias("severe_delay_rate"),
            count("*").alias("count"),
        ).collect()[0]

        if stats["count"] > 0:
            return {
                "condition": condition_name,
                "mean_delay": (
                    float(stats["mean_delay"]) if stats["mean_delay"] else 0.0
                ),
                "delay_rate_pct": (
                    float(stats["delay_rate"]) * 100 if stats["delay_rate"] else 0.0
                ),
                "sig_delay_rate_pct": (
                    float(stats["sig_delay_rate"]) * 100
                    if stats["sig_delay_rate"]
                    else 0.0
                ),
                "severe_delay_rate_pct": (
                    float(stats["severe_delay_rate"]) * 100
                    if stats["severe_delay_rate"]
                    else 0.0
                ),
                "n_records": int(stats["count"]),
            }
        return None

    # === EXTREME TEMPERATURE ===
    if "T_celsius" in df.columns:
        # Severe frost (<-5°C)
        severe_frost = df.filter(col("T_celsius") < -5)
        stat = get_stats(severe_frost, "Severe Frost (<-5°C)")
        if stat:
            results.append(stat)

        # Frost (<0°C)
        frost = df.filter(col("T_celsius") < 0)
        stat = get_stats(frost, "Frost (<0°C)")
        if stat:
            results.append(stat)

        # Heat wave (>30°C)
        heat = df.filter(col("T_celsius") > 30)
        stat = get_stats(heat, "Heat Wave (>30°C)")
        if stat:
            results.append(stat)

    # === EXTREME WIND ===
    if "FX_ms" in df.columns:
        # Storm force gusts (>20 m/s = 72 km/h)
        storm_gusts = df.filter(col("FX_ms") > 20)
        stat = get_stats(storm_gusts, "Storm Gusts (>20 m/s)")
        if stat:
            results.append(stat)

        # Severe storm gusts (>25 m/s = 90 km/h)
        severe_storm = df.filter(col("FX_ms") > 25)
        stat = get_stats(severe_storm, "Severe Storm (>25 m/s)")
        if stat:
            results.append(stat)

        # Hurricane force (>32.7 m/s = 118 km/h)
        hurricane = df.filter(col("FX_ms") > 32.7)
        stat = get_stats(hurricane, "Hurricane Force (>32.7 m/s)")
        if stat:
            results.append(stat)

    if "FF_ms" in df.columns:
        # Strong sustained wind (>15 m/s)
        strong_wind = df.filter(col("FF_ms") > 15)
        stat = get_stats(strong_wind, "Strong Wind (>15 m/s mean)")
        if stat:
            results.append(stat)

    # === EXTREME PRECIPITATION ===
    if "DR_minutes" in df.columns:
        # Continuous rain (>45 min/hour)
        continuous_rain = df.filter(col("DR_minutes") > 45)
        stat = get_stats(continuous_rain, "Continuous Rain (>45 min/hr)")
        if stat:
            results.append(stat)

    # === LOW VISIBILITY ===
    if "VV" in df.columns:
        # Very low visibility (<1km, VV codes < 50)
        low_vis = df.filter(col("VV") < 20)
        stat = get_stats(low_vis, "Very Low Visibility (<2km)")
        if stat:
            results.append(stat)

        # Fog (VV < 10)
        fog = df.filter(col("VV") < 10)
        stat = get_stats(fog, "Fog/Dense Fog (<1km)")
        if stat:
            results.append(stat)

    # === COMPOUND CONDITIONS (Most Dangerous) ===
    # Ice conditions: frost + precipitation
    if "T_celsius" in df.columns and "DR_minutes" in df.columns:
        ice_risk = df.filter(
            (col("T_celsius") < 2) & (col("T_celsius") > -5) & (col("DR_minutes") > 0)
        )
        stat = get_stats(ice_risk, "ICE RISK (near-freezing + rain)")
        if stat:
            results.append(stat)

        # Freezing rain
        freezing_rain = df.filter((col("T_celsius") < 0) & (col("DR_minutes") > 10))
        stat = get_stats(freezing_rain, "FREEZING RAIN (<0°C + rain)")
        if stat:
            results.append(stat)

    # Storm + rain
    if "FX_ms" in df.columns and "DR_minutes" in df.columns:
        storm_rain = df.filter((col("FX_ms") > 20) & (col("DR_minutes") > 10))
        stat = get_stats(storm_rain, "STORM + RAIN (gusts>20 + rain)")
        if stat:
            results.append(stat)

    # Winter storm: cold + wind + precipitation
    if (
        "T_celsius" in df.columns
        and "FX_ms" in df.columns
        and "DR_minutes" in df.columns
    ):
        winter_storm = df.filter(
            (col("T_celsius") < 2) & (col("FX_ms") > 15) & (col("DR_minutes") > 5)
        )
        stat = get_stats(winter_storm, "WINTER STORM (cold+wind+precip)")
        if stat:
            results.append(stat)

    if results:
        extreme_df = spark.createDataFrame(results)
        logger.log(f"\nTrue Extreme Weather Events: {len(results)} conditions analyzed")
        return extreme_df

    return None


def analyze_delay_severity_distribution(df):
    """Analyze how delay severity changes with weather conditions."""
    logger.section("DELAY SEVERITY DISTRIBUTION BY WEATHER")

    results = {}

    # For each weather category, show the distribution of delay severities
    categories = [
        ("temp_category", "Temperature"),
        ("wind_category", "Wind"),
        ("rain_category", "Precipitation"),
    ]

    for cat_col, cat_name in categories:
        if cat_col not in df.columns:
            continue

        severity_stats = (
            df.groupBy(cat_col)
            .agg(
                count("*").alias("total_count"),
                F.sum("stop_arrival_delay_binary").alias("any_delay_count"),
                F.sum("stop_arrival_delay_significant").alias("sig_delay_count"),
                F.sum("stop_arrival_delay_severe").alias("severe_delay_count"),
                F.sum("stop_arrival_delay_very_severe").alias("very_severe_count"),
                mean("stop_arrival_delay").alias("mean_delay"),
                F.expr("percentile_approx(stop_arrival_delay, 0.5)").alias(
                    "median_delay"
                ),
                F.expr("percentile_approx(stop_arrival_delay, 0.95)").alias(
                    "p95_delay"
                ),
                F.expr("percentile_approx(stop_arrival_delay, 0.99)").alias(
                    "p99_delay"
                ),
            )
            .withColumn(
                "any_delay_pct", col("any_delay_count") / col("total_count") * 100
            )
            .withColumn(
                "sig_delay_pct", col("sig_delay_count") / col("total_count") * 100
            )
            .withColumn(
                "severe_delay_pct", col("severe_delay_count") / col("total_count") * 100
            )
            .withColumn(
                "very_severe_pct", col("very_severe_count") / col("total_count") * 100
            )
            .orderBy(cat_col)
        )

        results[cat_name.lower()] = severity_stats
        logger.log(f"Calculated delay severity distribution by {cat_name}")

    return results


def save_results(
    corr_df,
    delay_stats,
    extreme_df,
    aggregations,
    output_path,
    corr_by_metric_df=None,
    true_extreme_df=None,
    severity_dist=None,
):
    """Save all analysis results to HDFS."""
    logger.section("SAVING RESULTS")

    # Save correlations (baseline)
    if corr_df is not None:
        corr_path = f"{output_path}/correlations"
        corr_df.coalesce(1).write.mode("overwrite").option("header", True).csv(
            corr_path
        )
        logger.log(f"Saved correlations to: {corr_path}")

    # Save correlations by delay metric (new)
    if corr_by_metric_df is not None:
        corr_metric_path = f"{output_path}/correlations_by_metric"
        corr_by_metric_df.coalesce(1).write.mode("overwrite").option(
            "header", True
        ).csv(corr_metric_path)
        logger.log(f"Saved correlations by metric to: {corr_metric_path}")

    # Save delay stats
    for name, stats_df in delay_stats.items():
        stats_path = f"{output_path}/delay_stats_{name}"
        stats_df.coalesce(1).write.mode("overwrite").option("header", True).csv(
            stats_path
        )
        logger.log(f"Saved {name} stats to: {stats_path}")

    # Save extreme weather analysis (baseline)
    if extreme_df is not None:
        extreme_path = f"{output_path}/extreme_weather"
        extreme_df.coalesce(1).write.mode("overwrite").option("header", True).csv(
            extreme_path
        )
        logger.log(f"Saved extreme weather analysis to: {extreme_path}")

    # Save true extreme events analysis (new)
    if true_extreme_df is not None:
        true_extreme_path = f"{output_path}/true_extreme_events"
        true_extreme_df.coalesce(1).write.mode("overwrite").option("header", True).csv(
            true_extreme_path
        )
        logger.log(f"Saved true extreme events to: {true_extreme_path}")

    # Save severity distribution (new)
    if severity_dist is not None:
        for name, dist_df in severity_dist.items():
            dist_path = f"{output_path}/severity_distribution_{name}"
            dist_df.coalesce(1).write.mode("overwrite").option("header", True).csv(
                dist_path
            )
            logger.log(f"Saved {name} severity distribution to: {dist_path}")

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
"""Main analysis pipeline."""
logger.section("WEATHER-DELAY CORRELATION ANALYSIS (PySpark)")

# Load data
df = load_data(DATA_PATH)
total_count = df.count()

# Cache the dataframe for repeated use
df = df.cache()

# Preprocess
df = preprocess_data(df)

# ============================================
# BASELINE ANALYSIS (Original)
# ============================================
logger.section("PART 1: BASELINE ANALYSIS")

# Calculate correlations
corr_df = calculate_correlations(df)
if corr_df is not None:
    logger.log("\nCorrelation Results (top 20):")
    # Collect and log correlations
    corr_rows = corr_df.limit(20).collect()
    for row in corr_rows:
        logger.log(f"  {row['weather_label']:40} vs {row['delay_variable']:25} r={row['correlation']:+.4f}")

# Calculate delay stats by category
delay_stats = calculate_delay_stats_by_category(df)

# Extreme weather analysis (baseline thresholds)
extreme_df = analyze_extreme_weather(df)

# Generate aggregated data for visualization
aggregations = generate_aggregated_data_for_plots(df)

# ============================================
# EXTENDED ANALYSIS (New)
# ============================================
logger.section("PART 2: EXTENDED ANALYSIS - DIFFERENT DELAY METRICS")

# Correlations with different delay severity thresholds
corr_by_metric_df = calculate_correlations_by_delay_metric(df)

# True extreme events with stricter thresholds
true_extreme_df = analyze_true_extreme_events(df)

# Delay severity distribution by weather
severity_dist = analyze_delay_severity_distribution(df)

# ============================================
# SAVE ALL RESULTS
# ============================================

# Save all results (baseline + extended)
save_results(
    corr_df,
    delay_stats,
    extreme_df,
    aggregations,
    OUTPUT_PATH,
    corr_by_metric_df=corr_by_metric_df,
    true_extreme_df=true_extreme_df,
    severity_dist=severity_dist,
)

# Print summary report
print_summary_report(corr_df, delay_stats, extreme_df, total_count)

logger.section("ANALYSIS COMPLETE")
logger.log(f"\nAll results saved to: {OUTPUT_PATH}")
logger.log("\nTo create visualizations, download the CSV files and run:")
logger.log("  python src/plot_weather_analysis.py")

# Save log to HDFS
logger.save_to_hdfs(spark, OUTPUT_PATH)

# Unpersist cached data
df.unpersist()

spark.stop()
