"""
Extended Weather-Delay Analysis Script (PySpark Version)

This script performs extended analysis of weather-delay correlations including:
- Correlations with different delay severity thresholds (>0, >5, >15, >30 min)
- True extreme weather events (severe frost, storms, hurricanes, etc.)
- Compound weather conditions (winter storms, freezing rain, etc.)
- Delay severity distributions by weather category

Run this script AFTER analyze_weather_delays.py for additional insights.

Weather variables analyzed (from KNMI):
- T, T10N, TD: Temperature variables (°C)
- U: Relative humidity (%)
- RH, DR: Precipitation amount (mm) and duration (hours)
- DD, FF, FH, FX: Wind direction and speed (m/s)
- VV, N: Visibility and cloud cover
- W1-W6: Weather phenomena indicators
- P, SQ, Q: Pressure, sunshine, radiation

Usage:
    spark-submit --deploy-mode cluster --driver-memory 4g --executor-memory 4g \\
        src/analyze_weather_delays_extended.py
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
        end_time = datetime.now()
        duration = end_time - self.start_time

        self.log("\n" + "=" * 70)
        self.log("JOB TIMING")
        self.log("=" * 70)
        self.log(f"Start time: {self.start_time}")
        self.log(f"End time: {end_time}")
        self.log(f"Duration: {duration}")

        log_content = "\n".join(self.log_lines)
        log_path = f"{output_path}/extended_analysis_log.txt"

        log_rdd = spark.sparkContext.parallelize([log_content], 1)

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
OUTPUT_PATH = "/user/s3544648/final_project/data/analysis/extended"

# All weather columns from KNMI dataset
WEATHER_COLS = [
    # Temperature
    "T", "T10N", "TD",
    # Humidity
    "U",
    # Precipitation
    "RH", "DR",
    # Wind
    "DD", "FF", "FH", "FX",
    # Visibility & clouds
    "VV", "N",
    # Weather phenomena
    "WW", "W1", "W2", "W3", "W5", "W6",
    # Pressure
    "P",
    # Sunshine
    "SQ", "Q",
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
    SparkSession.builder.appName("weather_delay_extended_analysis")
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

    return df


def preprocess_data(df):
    """Clean and preprocess the data for extended analysis."""
    logger.section("PREPROCESSING DATA")

    # Convert DR from hours to minutes for easier interpretation
    if "DR" in df.columns:
        df = df.withColumn("DR_minutes", col("DR").cast(DoubleType()) * 60)

    # Create binary delay indicators with MULTIPLE thresholds
    for delay_col in DELAY_COLS:
        if delay_col in df.columns:
            # Any delay (>0 min)
            df = df.withColumn(
                f"{delay_col}_binary", when(col(delay_col) > 0, 1).otherwise(0)
            )
            # Significant delay (>5 min)
            df = df.withColumn(
                f"{delay_col}_significant", when(col(delay_col) > 5, 1).otherwise(0)
            )
            # Severe delay (>15 min)
            df = df.withColumn(
                f"{delay_col}_severe", when(col(delay_col) > 15, 1).otherwise(0)
            )
            # Very severe delay (>30 min)
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

    # Create weather categories for grouped analysis
    if "T" in df.columns:
        df = df.withColumn(
            "temp_category",
            when(col("T") < -5, "Severe Frost (<-5°C)")
            .when(col("T") < 0, "Frost (-5 to 0°C)")
            .when(col("T") < 10, "Cold (0-10°C)")
            .when(col("T") < 20, "Mild (10-20°C)")
            .when(col("T") < 30, "Warm (20-30°C)")
            .otherwise("Hot (>30°C)"),
        )

    if "FF" in df.columns:
        df = df.withColumn(
            "wind_category",
            when(col("FF") < 3, "Calm (<3 m/s)")
            .when(col("FF") < 7, "Light (3-7 m/s)")
            .when(col("FF") < 12, "Moderate (7-12 m/s)")
            .when(col("FF") < 17, "Fresh (12-17 m/s)")
            .otherwise("Strong (>17 m/s)"),
        )

    if "DR_minutes" in df.columns:
        df = df.withColumn(
            "rain_category",
            when(col("DR_minutes") == 0, "No Rain")
            .when(col("DR_minutes") < 10, "Light (<10 min/hr)")
            .when(col("DR_minutes") < 30, "Moderate (10-30 min/hr)")
            .when(col("DR_minutes") < 50, "Heavy (30-50 min/hr)")
            .otherwise("Continuous (>50 min/hr)"),
        )

    if "VV" in df.columns:
        df = df.withColumn(
            "visibility_category",
            when(col("VV") < 10, "Dense Fog (<1km)")
            .when(col("VV") < 30, "Fog/Mist (1-3km)")
            .when(col("VV") < 50, "Hazy (3-5km)")
            .otherwise("Clear (>5km)"),
        )

    logger.log("Preprocessing complete.")
    return df


def calculate_correlations_by_delay_metric(df):
    """Calculate correlations for different delay severity levels."""
    logger.section("CORRELATIONS BY DELAY SEVERITY THRESHOLD")

    available_weather = [c for c in WEATHER_COLS if c in df.columns]

    # Different delay metrics to analyze
    delay_metrics = {
        "any_delay_gt0min": "stop_arrival_delay_binary",
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

        logger.log(f"\nProcessing: {metric_name}")

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
        logger.log(f"\nTotal correlations calculated: {len(results)}")
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
    logger.log("\nAnalyzing extreme temperature conditions...")
    if "T" in df.columns:
        conditions = [
            (col("T") < -10, "Extreme Frost (<-10°C)"),
            (col("T") < -5, "Severe Frost (<-5°C)"),
            (col("T") < 0, "Frost (<0°C)"),
            (col("T") > 30, "Heat Wave (>30°C)"),
            (col("T") > 35, "Extreme Heat (>35°C)"),
        ]
        for condition, name in conditions:
            stat = get_stats(df.filter(condition), name)
            if stat:
                results.append(stat)

    # Minimum ground temperature (can indicate frost risk)
    if "T10N" in df.columns:
        conditions = [
            (col("T10N") < -5, "Ground Frost (<-5°C at 10cm)"),
            (col("T10N") < 0, "Ground Near-Freezing (<0°C at 10cm)"),
        ]
        for condition, name in conditions:
            stat = get_stats(df.filter(condition), name)
            if stat:
                results.append(stat)

    # === EXTREME WIND ===
    logger.log("Analyzing extreme wind conditions...")
    if "FX" in df.columns:
        conditions = [
            (col("FX") > 15, "Strong Gusts (>15 m/s = 54 km/h)"),
            (col("FX") > 20, "Storm Gusts (>20 m/s = 72 km/h)"),
            (col("FX") > 25, "Severe Storm (>25 m/s = 90 km/h)"),
            (col("FX") > 32.7, "Hurricane Force (>32.7 m/s = 118 km/h)"),
        ]
        for condition, name in conditions:
            stat = get_stats(df.filter(condition), name)
            if stat:
                results.append(stat)

    if "FF" in df.columns:
        conditions = [
            (col("FF") > 10, "Strong Wind (>10 m/s mean)"),
            (col("FF") > 15, "Very Strong Wind (>15 m/s mean)"),
            (col("FF") > 20, "Storm Force Wind (>20 m/s mean)"),
        ]
        for condition, name in conditions:
            stat = get_stats(df.filter(condition), name)
            if stat:
                results.append(stat)

    # === EXTREME PRECIPITATION ===
    logger.log("Analyzing extreme precipitation conditions...")
    if "DR_minutes" in df.columns:
        conditions = [
            (col("DR_minutes") > 30, "Heavy Rain (>30 min/hr)"),
            (col("DR_minutes") > 45, "Continuous Rain (>45 min/hr)"),
            (col("DR_minutes") >= 60, "Non-stop Rain (full hour)"),
        ]
        for condition, name in conditions:
            stat = get_stats(df.filter(condition), name)
            if stat:
                results.append(stat)

    if "RH" in df.columns:
        conditions = [
            (col("RH") > 5, "Heavy Precipitation (>5mm/hr)"),
            (col("RH") > 10, "Very Heavy Precipitation (>10mm/hr)"),
        ]
        for condition, name in conditions:
            stat = get_stats(df.filter(condition), name)
            if stat:
                results.append(stat)

    # === LOW VISIBILITY ===
    logger.log("Analyzing visibility conditions...")
    if "VV" in df.columns:
        conditions = [
            (col("VV") < 50, "Reduced Visibility (<5km)"),
            (col("VV") < 30, "Poor Visibility (<3km)"),
            (col("VV") < 20, "Very Poor Visibility (<2km)"),
            (col("VV") < 10, "Fog (<1km)"),
            (col("VV") < 5, "Dense Fog (<500m)"),
        ]
        for condition, name in conditions:
            stat = get_stats(df.filter(condition), name)
            if stat:
                results.append(stat)

    # === WEATHER PHENOMENA INDICATORS ===
    logger.log("Analyzing weather phenomena...")
    phenomena = [
        ("W1", "Fog Present (W1=1)"),
        ("W2", "Rain Present (W2=1)"),
        ("W3", "Snow Present (W3=1)"),
        ("W5", "Thunder Present (W5=1)"),
        ("W6", "Ice Formation (W6=1)"),
    ]
    for indicator, name in phenomena:
        if indicator in df.columns:
            stat = get_stats(df.filter(col(indicator) == 1), name)
            if stat:
                results.append(stat)

    # === COMPOUND CONDITIONS (Most Dangerous) ===
    logger.log("Analyzing compound weather conditions...")

    # Ice conditions: near-freezing + precipitation
    if "T" in df.columns and "DR_minutes" in df.columns:
        conditions = [
            (
                (col("T") < 2) & (col("T") > -5) & (col("DR_minutes") > 0),
                "ICE RISK (near-freezing + any rain)",
            ),
            (
                (col("T") < 0) & (col("DR_minutes") > 10),
                "FREEZING RAIN (<0°C + rain >10min)",
            ),
            (
                (col("T") < -5) & (col("DR_minutes") > 0),
                "SEVERE ICE (<-5°C + precipitation)",
            ),
        ]
        for condition, name in conditions:
            stat = get_stats(df.filter(condition), name)
            if stat:
                results.append(stat)

    # Storm + rain
    if "FX" in df.columns and "DR_minutes" in df.columns:
        conditions = [
            (
                (col("FX") > 20) & (col("DR_minutes") > 10),
                "STORM + RAIN (gusts>20 + rain>10min)",
            ),
            (
                (col("FX") > 25) & (col("DR_minutes") > 20),
                "SEVERE STORM + RAIN (gusts>25 + rain>20min)",
            ),
        ]
        for condition, name in conditions:
            stat = get_stats(df.filter(condition), name)
            if stat:
                results.append(stat)

    # Winter storm: cold + wind + precipitation
    if "T" in df.columns and "FX" in df.columns and "DR_minutes" in df.columns:
        conditions = [
            (
                (col("T") < 2) & (col("FX") > 15) & (col("DR_minutes") > 5),
                "WINTER STORM (cold + wind + precip)",
            ),
            (
                (col("T") < 0) & (col("FX") > 20) & (col("DR_minutes") > 10),
                "SEVERE WINTER STORM",
            ),
        ]
        for condition, name in conditions:
            stat = get_stats(df.filter(condition), name)
            if stat:
                results.append(stat)

    # Low visibility + wind (dangerous for operations)
    if "VV" in df.columns and "FF" in df.columns:
        stat = get_stats(
            df.filter((col("VV") < 20) & (col("FF") > 10)),
            "FOG + WIND (visibility<2km + wind>10m/s)",
        )
        if stat:
            results.append(stat)

    # Snow conditions
    if "W3" in df.columns:
        if "FX" in df.columns:
            stat = get_stats(
                df.filter((col("W3") == 1) & (col("FX") > 15)),
                "BLIZZARD (snow + strong wind)",
            )
            if stat:
                results.append(stat)
        if "T" in df.columns:
            stat = get_stats(
                df.filter((col("W3") == 1) & (col("T") < -3)),
                "HEAVY SNOW (snow + cold <-3°C)",
            )
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

    categories = [
        ("temp_category", "temperature"),
        ("wind_category", "wind"),
        ("rain_category", "precipitation"),
        ("visibility_category", "visibility"),
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

        results[cat_name] = severity_stats
        logger.log(f"Calculated delay severity distribution by {cat_name.title()}")

    return results


def analyze_weather_indicators_impact(df):
    """Analyze impact of binary weather indicators on delays."""
    logger.section("WEATHER INDICATORS IMPACT ANALYSIS")

    results = []
    indicators = [
        ("W1", "Fog"),
        ("W2", "Rain"),
        ("W3", "Snow"),
        ("W5", "Thunder"),
        ("W6", "Ice"),
    ]

    for indicator, name in indicators:
        if indicator not in df.columns:
            continue

        # Get stats for when indicator is present vs absent
        for present, label in [(1, "Present"), (0, "Absent")]:
            filtered = df.filter(col(indicator) == present)
            stats = filtered.agg(
                count("*").alias("count"),
                mean("stop_arrival_delay").alias("mean_delay"),
                mean("stop_arrival_delay_binary").alias("delay_rate"),
                mean("stop_arrival_delay_significant").alias("sig_delay_rate"),
                mean("stop_arrival_delay_severe").alias("severe_delay_rate"),
            ).collect()[0]

            if stats["count"] > 0:
                results.append(
                    {
                        "indicator": indicator,
                        "weather_type": name,
                        "status": label,
                        "count": int(stats["count"]),
                        "mean_delay": float(stats["mean_delay"]) if stats["mean_delay"] else 0.0,
                        "delay_rate_pct": float(stats["delay_rate"]) * 100 if stats["delay_rate"] else 0.0,
                        "sig_delay_rate_pct": float(stats["sig_delay_rate"]) * 100 if stats["sig_delay_rate"] else 0.0,
                        "severe_delay_rate_pct": float(stats["severe_delay_rate"]) * 100 if stats["severe_delay_rate"] else 0.0,
                    }
                )

    if results:
        indicator_df = spark.createDataFrame(results)
        logger.log(f"\nWeather Indicators Analysis: {len(results)} records")
        return indicator_df

    return None


def save_results(
    corr_by_metric_df,
    true_extreme_df,
    severity_dist,
    indicators_df,
    output_path,
):
    """Save all extended analysis results to HDFS."""
    logger.section("SAVING RESULTS")

    # Save correlations by delay metric
    if corr_by_metric_df is not None:
        path = f"{output_path}/correlations_by_metric"
        corr_by_metric_df.coalesce(1).write.mode("overwrite").option(
            "header", True
        ).csv(path)
        logger.log(f"Saved correlations by metric to: {path}")

    # Save true extreme events analysis
    if true_extreme_df is not None:
        path = f"{output_path}/true_extreme_events"
        true_extreme_df.coalesce(1).write.mode("overwrite").option("header", True).csv(
            path
        )
        logger.log(f"Saved true extreme events to: {path}")

    # Save severity distribution
    if severity_dist is not None:
        for name, dist_df in severity_dist.items():
            path = f"{output_path}/severity_distribution_{name}"
            dist_df.coalesce(1).write.mode("overwrite").option("header", True).csv(
                path
            )
            logger.log(f"Saved {name} severity distribution to: {path}")

    # Save weather indicators analysis
    if indicators_df is not None:
        path = f"{output_path}/weather_indicators_impact"
        indicators_df.coalesce(1).write.mode("overwrite").option("header", True).csv(
            path
        )
        logger.log(f"Saved weather indicators analysis to: {path}")


def print_extended_summary(corr_by_metric_df, true_extreme_df, total_count):
    """Print summary of extended analysis."""
    logger.section("EXTENDED ANALYSIS SUMMARY")
    logger.log(f"\nDataset: {total_count:,} records")

    logger.subsection("1. CORRELATION BY DELAY SEVERITY")
    if corr_by_metric_df is not None:
        # Show how correlations change with delay threshold
        metrics = corr_by_metric_df.select("delay_metric").distinct().collect()
        for metric_row in metrics:
            metric = metric_row["delay_metric"]
            top_corr = (
                corr_by_metric_df.filter(col("delay_metric") == metric)
                .orderBy(col("abs_correlation").desc())
                .limit(5)
                .collect()
            )
            logger.log(f"\n  {metric}:")
            for row in top_corr:
                logger.log(f"    {row['weather_label']:35} r={row['correlation']:+.4f}")

    logger.subsection("2. EXTREME WEATHER IMPACT")
    if true_extreme_df is not None:
        # Show conditions with highest delay rates
        high_impact = (
            true_extreme_df.filter(col("condition") != "BASELINE (All Data)")
            .orderBy(col("delay_rate_pct").desc())
            .limit(10)
            .collect()
        )
        logger.log("\n  Top 10 conditions by delay rate:")
        for row in high_impact:
            logger.log(
                f"    {row['condition']:45} "
                f"delay_rate={row['delay_rate_pct']:.1f}% "
                f"(n={row['n_records']:,})"
            )


# === Main Analysis Pipeline ===
if __name__ == "__main__":
    logger.section("EXTENDED WEATHER-DELAY ANALYSIS (PySpark)")
    logger.log("Script: analyze_weather_delays_extended.py")

    # Load data
    df = load_data(DATA_PATH)
    total_count = df.count()

    # Cache the dataframe for repeated use
    df = df.cache()

    # Preprocess with multiple severity thresholds
    df = preprocess_data(df)

    # === EXTENDED ANALYSIS ===

    # 1. Correlations with different delay severity thresholds
    corr_by_metric_df = calculate_correlations_by_delay_metric(df)

    # 2. True extreme events with stricter thresholds
    true_extreme_df = analyze_true_extreme_events(df)

    # 3. Delay severity distribution by weather category
    severity_dist = analyze_delay_severity_distribution(df)

    # 4. Weather indicators impact analysis
    indicators_df = analyze_weather_indicators_impact(df)

    # Save all results
    save_results(
        corr_by_metric_df,
        true_extreme_df,
        severity_dist,
        indicators_df,
        OUTPUT_PATH,
    )

    # Print summary
    print_extended_summary(corr_by_metric_df, true_extreme_df, total_count)

    logger.section("EXTENDED ANALYSIS COMPLETE")
    logger.log(f"\nAll results saved to: {OUTPUT_PATH}")
    logger.log("\nTo create visualizations, download the CSV files and run:")
    logger.log("  python src/plot_weather_analysis.py")

    # Save log to HDFS
    logger.save_to_hdfs(spark, OUTPUT_PATH)

    # Unpersist cached data
    df.unpersist()

    spark.stop()
