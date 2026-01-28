# Compare train companies performances

from pyspark.sql import SparkSession

from pyspark.sql import functions as F

spark = SparkSession.builder.appName('Trains').getOrCreate()

data2019 = spark.read.option("header","true").csv(r"/user/s3692612/final_project/data/raw/services/services-2019.csv.gz")
data2020 = spark.read.option("header","true").csv(r"/user/s3692612/final_project/data/raw/services/services-2020.csv.gz")
data2021 = spark.read.option("header","true").csv(r"/user/s3692612/final_project/data/raw/services/services-2021.csv.gz")
data2022 = spark.read.option("header","true").csv(r"/user/s3692612/final_project/data/raw/services/services-2022.csv.gz")
data2023 = spark.read.option("header","true").csv(r"/user/s3692612/final_project/data/raw/services/services-2023.csv.gz")
data2024 = spark.read.option("header","true").csv(r"/user/s3692612/final_project/data/raw/services/services-2024.csv.gz")
data2025 = spark.read \
    .option("header", "true") \
    .csv([
        r"/user/s3692612/final_project/data/raw/services/services-2025-01.csv.gz",
        r"/user/s3692612/final_project/data/raw/services/services-2025-02.csv.gz",
        r"/user/s3692612/final_project/data/raw/services/services-2025-03.csv.gz",
        r"/user/s3692612/final_project/data/raw/services/services-2025-04.csv.gz",
        r"/user/s3692612/final_project/data/raw/services/services-2025-05.csv.gz",
        r"/user/s3692612/final_project/data/raw/services/services-2025-06.csv.gz",
        r"/user/s3692612/final_project/data/raw/services/services-2025-07.csv.gz",
        r"/user/s3692612/final_project/data/raw/services/services-2025-08.csv.gz",
        r"/user/s3692612/final_project/data/raw/services/services-2025-09.csv.gz",
        r"/user/s3692612/final_project/data/raw/services/services-2025-10.csv.gz",
        r"/user/s3692612/final_project/data/raw/services/services-2025-11.csv.gz"
    ])

data_all_years = (
    data2019.withColumn("year", F.lit(2019))
    .unionByName(data2020.withColumn("year", F.lit(2020)))
    .unionByName(data2021.withColumn("year", F.lit(2021)))
    .unionByName(data2022.withColumn("year", F.lit(2022)))
    .unionByName(data2023.withColumn("year", F.lit(2023)))
    .unionByName(data2024.withColumn("year", F.lit(2024)))
    .unionByName(data2025.withColumn("year", F.lit(2025)))
)

data_all_years = data_all_years.withColumn(
    "effective_delay",
    F.coalesce('Stop:Arrival delay', 'Stop:Departure delay')
)

per_rdt = (
    data_all_years
    .groupBy(
        "year",
        'Service:Company',
        'Service:RDT-ID',
        'Service:Completely cancelled',
        'Service:Partly cancelled'
    )
    .agg(
        F.avg("effective_delay").alias("avg_delay_per_service")
    )
)

per_rdt = (
    per_rdt
    .withColumn(
        "service_status",
        F.when(F.col("Service:Completely cancelled") == True, "Service:Completely cancelled")
         .when(F.col('Service:Partly cancelled') == True, 'Service:Partly cancelled')
         .otherwise("not cancelled")
    )
)

avg_delay_by_company_year = (
    per_rdt
    .groupBy("year", "Service:Company")
    .agg(
        F.avg("avg_delay_per_service").alias("avg_delay_minutes")
    )
)

status_counts = (
    per_rdt
    .groupBy("year", "Service:Company", "service_status")
    .count()
)

total_counts = (
    per_rdt
    .groupBy("year", "Service:Company")
    .count()
    .withColumnRenamed("count", "total_services")
)

ratios = (
    status_counts
    .join(total_counts, ["year", "Service:Company"])
    .withColumn(
        "ratio",
        F.col("count") / F.col("total_services")
    )
)

ratios_pivot = (
    ratios
    .groupBy("year", "Service:Company")
    .pivot("service_status", ["Service:Completely cancelled", "Service:Partly cancelled", "not cancelled"])
    .agg(F.first("ratio"))
)

final_by_year = (
    avg_delay_by_company_year
    .join(ratios_pivot, ["year", "Service:Company"], "left")
)

all_years = (
    per_rdt
    .groupBy("Service:Company")
    .agg(
        F.avg("avg_delay_per_service").alias("avg_delay_minutes"),
        F.count("*").alias("total_services"),
        F.sum(F.when(F.col("service_status") == "Service:Completely cancelled", 1).otherwise(0)).alias("Service:Completely cancelled"),
        F.sum(F.when(F.col("service_status") == "Service:Partly cancelled", 1).otherwise(0)).alias("Service:Partly cancelled"),
        F.sum(F.when(F.col("service_status") == "not cancelled", 1).otherwise(0)).alias("not cancelled")
    )
    .withColumn("year", F.lit("ALL_YEARS"))
    .withColumn("Service:Completely cancelled", F.col("Service:Completely cancelled") / F.col("total_services"))
    .withColumn("Service:Partly cancelled", F.col("Service:Partly cancelled") / F.col("total_services"))
    .withColumn("not cancelled", F.col("not cancelled") / F.col("total_services"))
    .drop("total_services")
)

final_output = final_by_year.unionByName(all_years)

final_output \
    .coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("/user/s3063496/final_project/results/service_company_summary_by_year")
