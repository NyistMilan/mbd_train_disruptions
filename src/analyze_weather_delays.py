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
    SparkSession.builder
    .appName("weather_delay_correlation_analysis")
    .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


def load_data(data_path: str):
    """Load the services_with_weather parquet data."""
    print(f"\n{'='*70}")
    print("LOADING DATA")
    print(f"{'='*70}")
    print(f"Loading data from {data_path}...")

    df = spark.read.parquet(data_path)

    total_count = df.count()
    print(f"Loaded {total_count:,} records")
    print(f"Columns: {df.columns}")

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

    # Create binary delay indicators
    for delay_col in DELAY_COLS:
        if delay_col in df.columns:
            df = df.withColumn(
                f"{delay_col}_binary",
                when(col(delay_col) > 0, 1).otherwise(0)
            )
            df = df.withColumn(
                f"{delay_col}_significant",
                when(col(delay_col) > 5, 1).otherwise(0)
            )

    # Create weather categories
    if "T_celsius" in df.columns:
        df = df.withColumn(
            "temp_category",
            when(col("T_celsius") < 0, "Freezing (<0°C)")
            .when(col("T_celsius") < 10, "Cold (0-10°C)")
            .when(col("T_celsius") < 20, "Mild (10-20°C)")
            .otherwise("Warm (>20°C)")
        )

    if "FF_ms" in df.columns:
        df = df.withColumn(
            "wind_category",
            when(col("FF_ms") < 5, "Calm (<5 m/s)")
            .when(col("FF_ms") < 10, "Light (5-10 m/s)")
            .when(col("FF_ms") < 15, "Moderate (10-15 m/s)")
            .otherwise("Strong (>15 m/s)")
        )

    if "DR_minutes" in df.columns:
        df = df.withColumn(
            "rain_category",
            when(col("DR_minutes") == 0, "No Rain")
            .when(col("DR_minutes") < 10, "Light Rain")
            .when(col("DR_minutes") < 30, "Moderate Rain")
            .otherwise("Heavy Rain")
        )

    print("Preprocessing complete.")
    return df


def calculate_correlations(df):
    """Calculate correlations between weather variables and delays using Spark."""
    print(f"\n{'='*70}")
    print("CALCULATING CORRELATIONS")
    print(f"{'='*70}")

    # Get available columns
    available_weather = [c for c in WEATHER_COLS if c in df.columns]
    available_delay = [c for c in DELAY_COLS if c in df.columns]

    print(f"Weather columns: {available_weather}")
    print(f"Delay columns: {available_delay}")

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
                corr(col(weather_col).cast(DoubleType()), 
                     col(delay_col).cast(DoubleType()))
            ).collect()[0][0]

            if correlation is not None:
                results.append({
                    "weather_variable": weather_col,
                    "weather_label": WEATHER_LABELS.get(weather_col, weather_col),
                    "delay_variable": delay_col,
                    "n_samples": n_samples,
                    "correlation": correlation,
                    "abs_correlation": abs(correlation)
                })

    # Create DataFrame and sort by absolute correlation
    if results:
        corr_df = spark.createDataFrame(results)
        corr_df = corr_df.orderBy(col("abs_correlation").desc())
        return corr_df
    
    return None


def calculate_delay_stats_by_category(df):
    """Calculate delay statistics for different weather conditions."""
    print(f"\n{'='*70}")
    print("CALCULATING DELAY RATES BY WEATHER CONDITION")
    print(f"{'='*70}")

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
                mean("stop_arrival_delay_significant").alias("significant_delay_rate")
            )
            .orderBy("temp_category")
        )
        results["temperature"] = temp_stats
        print("\nDelay by Temperature Category:")
        temp_stats.show(truncate=False)

    # Stats by wind category
    if "wind_category" in df.columns and "stop_arrival_delay" in df.columns:
        wind_stats = (
            df.groupBy("wind_category")
            .agg(
                count("*").alias("count"),
                mean("stop_arrival_delay").alias("mean_delay"),
                stddev("stop_arrival_delay").alias("std_delay"),
                mean("stop_arrival_delay_binary").alias("delay_rate"),
                mean("stop_arrival_delay_significant").alias("significant_delay_rate")
            )
            .orderBy("wind_category")
        )
        results["wind"] = wind_stats
        print("\nDelay by Wind Category:")
        wind_stats.show(truncate=False)

    # Stats by precipitation category
    if "rain_category" in df.columns and "stop_arrival_delay" in df.columns:
        rain_stats = (
            df.groupBy("rain_category")
            .agg(
                count("*").alias("count"),
                mean("stop_arrival_delay").alias("mean_delay"),
                stddev("stop_arrival_delay").alias("std_delay"),
                mean("stop_arrival_delay_binary").alias("delay_rate"),
                mean("stop_arrival_delay_significant").alias("significant_delay_rate")
            )
            .orderBy("rain_category")
        )
        results["precipitation"] = rain_stats
        print("\nDelay by Precipitation Category:")
        rain_stats.show(truncate=False)

    return results


def analyze_extreme_weather(df):
    """Analyze delay patterns during extreme weather conditions."""
    print(f"\n{'='*70}")
    print("EXTREME WEATHER ANALYSIS")
    print(f"{'='*70}")

    results = []

    # Extreme cold (< 0°C) vs Normal (5-20°C)
    if "T_celsius" in df.columns:
        freezing = df.filter(col("T_celsius") < 0)
        normal_temp = df.filter((col("T_celsius") >= 5) & (col("T_celsius") <= 20))

        freezing_stats = freezing.agg(
            mean("stop_arrival_delay").alias("mean_delay"),
            mean("stop_arrival_delay_binary").alias("delay_rate"),
            count("*").alias("count")
        ).collect()[0]

        normal_temp_stats = normal_temp.agg(
            mean("stop_arrival_delay").alias("mean_delay"),
            mean("stop_arrival_delay_binary").alias("delay_rate"),
            count("*").alias("count")
        ).collect()[0]

        if freezing_stats["count"] > 0:
            results.append({
                "condition": "Freezing (<0°C)",
                "mean_delay": float(freezing_stats["mean_delay"]) if freezing_stats["mean_delay"] else 0.0,
                "delay_rate_pct": float(freezing_stats["delay_rate"]) * 100 if freezing_stats["delay_rate"] else 0.0,
                "n_records": int(freezing_stats["count"])
            })
        if normal_temp_stats["count"] > 0:
            results.append({
                "condition": "Normal Temp (5-20°C)",
                "mean_delay": float(normal_temp_stats["mean_delay"]) if normal_temp_stats["mean_delay"] else 0.0,
                "delay_rate_pct": float(normal_temp_stats["delay_rate"]) * 100 if normal_temp_stats["delay_rate"] else 0.0,
                "n_records": int(normal_temp_stats["count"])
            })

    # High wind (> 15 m/s) vs Calm (< 5 m/s)
    if "FF_ms" in df.columns:
        high_wind = df.filter(col("FF_ms") > 15)
        calm = df.filter(col("FF_ms") < 5)

        high_wind_stats = high_wind.agg(
            mean("stop_arrival_delay").alias("mean_delay"),
            mean("stop_arrival_delay_binary").alias("delay_rate"),
            count("*").alias("count")
        ).collect()[0]

        calm_stats = calm.agg(
            mean("stop_arrival_delay").alias("mean_delay"),
            mean("stop_arrival_delay_binary").alias("delay_rate"),
            count("*").alias("count")
        ).collect()[0]

        if high_wind_stats["count"] > 0:
            results.append({
                "condition": "Strong Wind (>15 m/s)",
                "mean_delay": float(high_wind_stats["mean_delay"]) if high_wind_stats["mean_delay"] else 0.0,
                "delay_rate_pct": float(high_wind_stats["delay_rate"]) * 100 if high_wind_stats["delay_rate"] else 0.0,
                "n_records": int(high_wind_stats["count"])
            })
        if calm_stats["count"] > 0:
            results.append({
                "condition": "Calm (<5 m/s)",
                "mean_delay": float(calm_stats["mean_delay"]) if calm_stats["mean_delay"] else 0.0,
                "delay_rate_pct": float(calm_stats["delay_rate"]) * 100 if calm_stats["delay_rate"] else 0.0,
                "n_records": int(calm_stats["count"])
            })

    # Heavy rain vs No rain
    if "DR_minutes" in df.columns:
        heavy_rain = df.filter(col("DR_minutes") > 30)
        no_rain = df.filter(col("DR_minutes") == 0)

        heavy_rain_stats = heavy_rain.agg(
            mean("stop_arrival_delay").alias("mean_delay"),
            mean("stop_arrival_delay_binary").alias("delay_rate"),
            count("*").alias("count")
        ).collect()[0]

        no_rain_stats = no_rain.agg(
            mean("stop_arrival_delay").alias("mean_delay"),
            mean("stop_arrival_delay_binary").alias("delay_rate"),
            count("*").alias("count")
        ).collect()[0]

        if heavy_rain_stats["count"] > 0:
            results.append({
                "condition": "Heavy Rain (>30 min/hr)",
                "mean_delay": float(heavy_rain_stats["mean_delay"]) if heavy_rain_stats["mean_delay"] else 0.0,
                "delay_rate_pct": float(heavy_rain_stats["delay_rate"]) * 100 if heavy_rain_stats["delay_rate"] else 0.0,
                "n_records": int(heavy_rain_stats["count"])
            })
        if no_rain_stats["count"] > 0:
            results.append({
                "condition": "No Rain",
                "mean_delay": float(no_rain_stats["mean_delay"]) if no_rain_stats["mean_delay"] else 0.0,
                "delay_rate_pct": float(no_rain_stats["delay_rate"]) * 100 if no_rain_stats["delay_rate"] else 0.0,
                "n_records": int(no_rain_stats["count"])
            })

    if results:
        extreme_df = spark.createDataFrame(results)
        print("\nExtreme Weather Impact:")
        extreme_df.show(truncate=False)
        return extreme_df

    return None


def generate_aggregated_data_for_plots(df):
    """Generate aggregated data that can be used for plotting locally."""
    print(f"\n{'='*70}")
    print("GENERATING AGGREGATED DATA FOR VISUALIZATION")
    print(f"{'='*70}")

    aggregations = {}

    # 1. Hourly aggregation by weather bins for scatter-like visualization
    if "T_celsius" in df.columns:
        temp_bins = (
            df.withColumn(
                "temp_bin",
                (F.floor(col("T_celsius") / 2) * 2).cast("int")  # 2°C bins
            )
            .groupBy("temp_bin")
            .agg(
                mean("stop_arrival_delay").alias("mean_delay"),
                stddev("stop_arrival_delay").alias("std_delay"),
                count("*").alias("count")
            )
            .orderBy("temp_bin")
        )
        aggregations["temp_bins"] = temp_bins

    if "FF_ms" in df.columns:
        wind_bins = (
            df.withColumn(
                "wind_bin",
                (F.floor(col("FF_ms"))).cast("int")  # 1 m/s bins
            )
            .groupBy("wind_bin")
            .agg(
                mean("stop_arrival_delay").alias("mean_delay"),
                stddev("stop_arrival_delay").alias("std_delay"),
                count("*").alias("count")
            )
            .orderBy("wind_bin")
        )
        aggregations["wind_bins"] = wind_bins

    if "DR_minutes" in df.columns:
        rain_bins = (
            df.withColumn(
                "rain_bin",
                (F.floor(col("DR_minutes") / 5) * 5).cast("int")  # 5 min bins
            )
            .groupBy("rain_bin")
            .agg(
                mean("stop_arrival_delay").alias("mean_delay"),
                stddev("stop_arrival_delay").alias("std_delay"),
                count("*").alias("count")
            )
            .orderBy("rain_bin")
        )
        aggregations["rain_bins"] = rain_bins

    if "VV" in df.columns:
        visibility_bins = (
            df.withColumn(
                "visibility_bin",
                (F.floor(col("VV") / 10) * 10).cast("int")  # 10-unit bins
            )
            .groupBy("visibility_bin")
            .agg(
                mean("stop_arrival_delay").alias("mean_delay"),
                stddev("stop_arrival_delay").alias("std_delay"),
                count("*").alias("count")
            )
            .orderBy("visibility_bin")
        )
        aggregations["visibility_bins"] = visibility_bins

    return aggregations


def save_results(corr_df, delay_stats, extreme_df, aggregations, output_path):
    """Save all analysis results to HDFS."""
    print(f"\n{'='*70}")
    print("SAVING RESULTS")
    print(f"{'='*70}")

    # Save correlations
    if corr_df is not None:
        corr_path = f"{output_path}/correlations"
        corr_df.coalesce(1).write.mode("overwrite").option("header", True).csv(corr_path)
        print(f"Saved correlations to: {corr_path}")

    # Save delay stats
    for name, stats_df in delay_stats.items():
        stats_path = f"{output_path}/delay_stats_{name}"
        stats_df.coalesce(1).write.mode("overwrite").option("header", True).csv(stats_path)
        print(f"Saved {name} stats to: {stats_path}")

    # Save extreme weather analysis
    if extreme_df is not None:
        extreme_path = f"{output_path}/extreme_weather"
        extreme_df.coalesce(1).write.mode("overwrite").option("header", True).csv(extreme_path)
        print(f"Saved extreme weather analysis to: {extreme_path}")

    # Save aggregated data for plotting
    for name, agg_df in aggregations.items():
        agg_path = f"{output_path}/aggregated_{name}"
        agg_df.coalesce(1).write.mode("overwrite").option("header", True).csv(agg_path)
        print(f"Saved aggregated {name} to: {agg_path}")


def print_summary_report(corr_df, delay_stats, extreme_df, total_count):
    """Print a summary report of the analysis."""
    print(f"\n{'='*70}")
    print("WEATHER-DELAY CORRELATION ANALYSIS REPORT")
    print(f"{'='*70}")
    print(f"\nDataset: {total_count:,} train service records with weather data")

    print(f"\n{'-'*70}")
    print("1. CORRELATION SUMMARY")
    print(f"{'-'*70}")
    print("\nTop correlations between weather variables and arrival delays:")

    if corr_df is not None:
        arrival_corr = (
            corr_df.filter(col("delay_variable") == "stop_arrival_delay")
            .orderBy(col("abs_correlation").desc())
            .limit(10)
        )

        rows = arrival_corr.collect()
        for row in rows:
            print(f"  {row['weather_label']:40} r={row['correlation']:+.4f}")

    print("\nINTERPRETATION:")
    print("  - Positive correlation: Higher values → more delays")
    print("  - Negative correlation: Higher values → fewer delays")
    print("  - |r| < 0.1: Very weak correlation")
    print("  - 0.1 ≤ |r| < 0.3: Weak correlation")
    print("  - 0.3 ≤ |r| < 0.5: Moderate correlation")
    print("  - |r| ≥ 0.5: Strong correlation")

    print(f"\n{'-'*70}")
    print("2. KEY FINDINGS")
    print(f"{'-'*70}")

    if corr_df is not None:
        # Get mean absolute correlation
        mean_corr = corr_df.filter(
            col("delay_variable") == "stop_arrival_delay"
        ).agg(mean("abs_correlation")).collect()[0][0]

        if mean_corr:
            print(f"\n  • Average absolute correlation: {mean_corr:.4f}")

            if mean_corr < 0.1:
                strength = "very weak"
            elif mean_corr < 0.3:
                strength = "weak"
            else:
                strength = "moderate"

            print(f"\n  CONCLUSION:")
            print(f"    Weather conditions show a {strength} correlation with train delays.")
            print("    Other factors (infrastructure, scheduling, incidents) likely play")
            print("    a more significant role in causing delays.")

    print(f"\n{'='*70}")


def main():
    """Main analysis pipeline."""
    print("=" * 70)
    print("WEATHER-DELAY CORRELATION ANALYSIS (PySpark)")
    print("=" * 70)

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
        print("\nCorrelation Results:")
        corr_df.show(20, truncate=False)

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

    print(f"\n{'='*70}")
    print("ANALYSIS COMPLETE")
    print(f"{'='*70}")
    print(f"\nAll results saved to: {OUTPUT_PATH}")
    print("\nTo create visualizations, download the CSV files and run:")
    print("  python src/plot_weather_analysis.py")

    # Unpersist cached data
    df.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()
