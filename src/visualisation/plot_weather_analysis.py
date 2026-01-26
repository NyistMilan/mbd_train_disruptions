import os
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import t

# Configuration
DATA_DIR = Path("./analysis_data")  # Downloaded from HDFS
OUTPUT_DIR = Path("./analysis_output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

plt.style.use("seaborn-v0_8-whitegrid")
plt.rcParams["figure.figsize"] = (12, 6)
plt.rcParams["font.size"] = 11


def load_csv_from_spark_output(folder_path: Path) -> pd.DataFrame:
    """Load CSV from Spark output folder (handles part-* files)."""
    if not folder_path.exists():
        print(f"Warning: {folder_path} does not exist")
        return None

    # Find the CSV file (Spark outputs as part-*.csv)
    csv_files = list(folder_path.glob("part-*.csv"))
    if not csv_files:
        csv_files = list(folder_path.glob("*.csv"))

    if not csv_files:
        print(f"Warning: No CSV files found in {folder_path}")
        return None

    return pd.read_csv(csv_files[0])


def plot_correlation_heatmap(corr_df: pd.DataFrame, output_dir: Path):
    """Create a heatmap of weather-delay correlations."""
    if corr_df is None or corr_df.empty:
        print("No correlation data available")
        return

    print("Creating correlation heatmap...")

    # Pivot for heatmap
    pivot = corr_df.pivot_table(
        index="weather_variable", columns="delay_variable", values="correlation"
    )

    fig, ax = plt.subplots(figsize=(10, 8))
    sns.heatmap(
        pivot,
        annot=True,
        cmap="RdBu_r",
        center=0,
        fmt=".3f",
        vmin=-0.3,
        vmax=0.3,
        ax=ax,
        linewidths=0.5,
    )
    ax.set_title(
        "Weather-Delay Correlations\n(Pearson r)", fontsize=14, fontweight="bold"
    )
    ax.set_xlabel("Delay Variable", fontsize=12)
    ax.set_ylabel("Weather Variable", fontsize=12)

    plt.tight_layout()
    plt.savefig(output_dir / "correlation_heatmap.png", dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Saved: {output_dir / 'correlation_heatmap.png'}")


def plot_correlation_significance_heatmap(corr_df: pd.DataFrame, output_dir: Path):
    """Create a heatmap of weather-delay correlation significance (p-values)."""
    if corr_df is None or corr_df.empty:
        print("No correlation data available")
        return

    print("Creating correlation significance heatmap...")

    alpha = 0.05

    # calculate p-values from correlation coefficients
    corr_df["t"] = (
        corr_df["correlation"]
        * np.sqrt(corr_df["n_samples"] - 2)
        / np.sqrt(1 - corr_df["correlation"] ** 2)
    )

    corr_df["p_value"] = 2 * (
        1 - t.cdf(np.abs(corr_df["t"]), df=corr_df["n_samples"] - 2)
    )

    # Pivot for heatmap
    pivot = corr_df.pivot_table(
        index="weather_variable", columns="delay_variable", values="p_value"
    )

    fig, ax = plt.subplots(figsize=(10, 8))
    sns.heatmap(
        pivot,
        annot=True,
        cmap="YlGnBu",
        fmt=".3f",
        vmin=0,
        vmax=0.1,
        ax=ax,
        linewidths=0.5,
    )
    ax.set_title(
        "Weather-Delay Correlation Significance\n(P-values)",
        fontsize=14,
        fontweight="bold",
    )
    ax.set_xlabel("Delay Variable", fontsize=12)
    ax.set_ylabel("Weather Variable", fontsize=12)

    plt.tight_layout()
    plt.savefig(
        output_dir / "correlation_significance_heatmap.png",
        dpi=150,
        bbox_inches="tight",
    )
    plt.close()
    print(f"Saved: {output_dir / 'correlation_significance_heatmap.png'}")


def plot_correlation_bar(corr_df: pd.DataFrame, output_dir: Path):
    """Create a bar chart of correlations for arrival delay."""
    if corr_df is None or corr_df.empty:
        print("No correlation data available")
        return

    print("Creating correlation bar chart...")

    # Filter for arrival delay only
    arrival_corr = corr_df[corr_df["delay_variable"] == "stop_arrival_delay"].copy()
    arrival_corr = arrival_corr.sort_values("abs_correlation")
    # take top 10 by absolute correlation
    arrival_corr = arrival_corr.tail(5)
    arrival_corr = arrival_corr.sort_values("correlation")

    fig, ax = plt.subplots(figsize=(10, 6))

    colors = ["red" if x > 0 else "blue" for x in arrival_corr["correlation"]]
    bars = ax.barh(
        arrival_corr["weather_label"],
        arrival_corr["correlation"],
        color=colors,
        alpha=0.7,
    )

    ax.axvline(x=0, color="black", linewidth=0.5)
    ax.set_xlabel("Pearson Correlation Coefficient (r)", fontsize=12)
    ax.set_title(
        "Weather Variables Correlation with Arrival Delay",
        fontsize=14,
        fontweight="bold",
    )

    # Add correlation values on bars
    for bar, val in zip(bars, arrival_corr["correlation"]):
        x_pos = val + 0.005 if val >= 0 else val - 0.005
        ha = "left" if val >= 0 else "right"
        ax.text(
            x_pos,
            bar.get_y() + bar.get_height() / 2,
            f"{val:.3f}",
            va="center",
            ha=ha,
            fontsize=9,
        )

    plt.tight_layout()
    plt.savefig(output_dir / "correlation_bar.png", dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Saved: {output_dir / 'correlation_bar.png'}")


def plot_aggregated_trends(aggregations: dict, output_dir: Path):
    """Plot aggregated weather vs delay trends."""
    print("Creating aggregated trend plots...")

    plots = [
        ("ww_bins", "ww_bin", "Weather Code (WMO 4680)", "Mean Delay vs Weather Code"),
        (
            "rain_bins",
            "rain_bin",
            "Precipitation Duration (min)",
            "Mean Delay vs Precipitation",
        ),
        (
            "sunshine_bins",
            "sunshine_bin",
            "Sunshine Duration (min)",
            "Mean Delay vs Sunshine",
        ),
        (
            "solar_bins",
            "solar_bin",
            "Solar Radiation (W/m²)",
            "Mean Delay vs Solar Radiation",
        ),
        ("temp_bins", "temp_bin", "Temperature (°C)", "Mean Delay vs Temperature"),
    ]

    available = [
        (k, c, l, t)
        for k, c, l, t in plots
        if k in aggregations and aggregations[k] is not None
    ]

    if not available:
        print("No aggregated data available for trend plots")
        return

    n_plots = len(available)
    n_cols = 3
    n_rows = 2
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(4 * n_cols, 4 * n_rows))
    axes = axes.flatten()

    for idx, (key, col, xlabel, title) in enumerate(available):
        ax = axes[idx]
        data = aggregations[key]
        if data.empty:
            ax.axis("off")
            continue

        # Filter to bins with enough samples
        data = data[data["count"] >= 100].copy()

        if len(data) < 2:
            ax.text(
                0.5,
                0.5,
                "Insufficient data",
                ha="center",
                va="center",
                transform=ax.transAxes,
            )
            ax.set_xlabel(xlabel)
            ax.set_ylabel("Mean Delay (min)")
            ax.set_title(title)
            ax.set_ylim(bottom=0)
            continue

        # Handle NaN values
        data = data.dropna(subset=[col, "mean_delay"])

        ax.errorbar(
            data[col],
            data["mean_delay"],
            yerr=data["std_delay"].fillna(0) / np.sqrt(data["count"]),
            fmt="o-",
            capsize=3,
            capthick=1,
            markersize=4,
        )

        ax.set_xlabel(xlabel)
        ax.set_ylabel("Mean Delay (min)")
        ax.set_title(title)
        ax.set_ylim(bottom=0)

        # Add trend line with error handling
        if len(data) >= 3:
            try:
                x_vals = data[col].astype(float).values
                y_vals = data["mean_delay"].astype(float).values
                if np.all(np.isfinite(x_vals)) and np.all(np.isfinite(y_vals)):
                    z = np.polyfit(x_vals, y_vals, 1)
                    p = np.poly1d(z)
                    x_line = np.linspace(x_vals.min(), x_vals.max(), 100)
                    ax.plot(x_line, p(x_line), "r--", alpha=0.7, label="Trend")
                    ax.legend()
            except (np.linalg.LinAlgError, ValueError) as e:
                print(f"  Warning: Could not fit trend line for {key}: {e}")

    # Hide unused subplots if there are fewer than 6 plots
    for i in range(len(available), n_rows * n_cols):
        axes[i].axis("off")

    plt.suptitle(
        "Weather-Delay Relationships (Aggregated)",
        fontsize=14,
        fontweight="bold",
        y=1.02,
    )
    plt.tight_layout()
    plt.savefig(output_dir / "weather_delay_trends.png", dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Saved: {output_dir / 'weather_delay_trends.png'}")


def main():
    if not DATA_DIR.exists():
        print(f"\nERROR: Data directory not found: {DATA_DIR}")
        print(f"  hdfs dfs -get /user/s3544648/final_project/data/analysis {DATA_DIR}")
        return

    # Load all data
    print("\nLoading data...")

    corr_df = load_csv_from_spark_output(DATA_DIR / "correlations")
    if corr_df is not None:
        print(f"  Loaded correlations: {len(corr_df)} rows")

    aggregations = {}
    for agg_type in [
        "ww_bins",
        "wind_bins",
        "rain_bins",
        "visibility_bins",
        "solar_bins",
        "sunshine_bins",
        "rain_indicator",
    ]:
        aggregations[agg_type] = load_csv_from_spark_output(
            DATA_DIR / f"aggregated_{agg_type}"
        )
        if aggregations[agg_type] is not None:
            print(f"  Loaded {agg_type}: {len(aggregations[agg_type])} rows")

    plot_correlation_heatmap(corr_df, OUTPUT_DIR)
    plot_correlation_significance_heatmap(corr_df, OUTPUT_DIR)
    plot_correlation_bar(corr_df, OUTPUT_DIR)
    plot_aggregated_trends(aggregations, OUTPUT_DIR)

    print("\n" + "=" * 70)
    print("VISUALIZATION COMPLETE")
    print("=" * 70)
    print(f"\nAll plots saved to: {OUTPUT_DIR}")
    for f in sorted(OUTPUT_DIR.glob("*.png")):
        print(f"  - {f.name}")


if __name__ == "__main__":
    main()
