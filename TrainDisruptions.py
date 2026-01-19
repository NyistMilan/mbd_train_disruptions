# Disruptions, their causes, graphing

from pyspark.sql import SparkSession

from pyspark.sql import functions as F

spark = SparkSession.builder.appName('Train disruptions').getOrCreate()

data2019 = spark.read.option("header","true").csv(r"/user/s3692612/final_project/data/raw/disruptions/disruptions-2019.csv")
data2020 = spark.read.option("header","true").csv(r"/user/s3692612/final_project/data/raw/disruptions/disruptions-2020.csv")
data2021 = spark.read.option("header","true").csv(r"/user/s3692612/final_project/data/raw/disruptions/disruptions-2021.csv")
data2022 = spark.read.option("header","true").csv(r"/user/s3692612/final_project/data/raw/disruptions/disruptions-2022.csv")
data2023 = spark.read.option("header","true").csv(r"/user/s3692612/final_project/data/raw/disruptions/disruptions-2023.csv")
data2024 = spark.read.option("header","true").csv(r"/user/s3692612/final_project/data/raw/disruptions/disruptions-2024.csv")



data_with_years = (
    data2019.withColumn("year", F.lit(2019))
    .unionByName(data2020.withColumn("year", F.lit(2020)))
    .unionByName(data2021.withColumn("year", F.lit(2021)))
    .unionByName(data2022.withColumn("year", F.lit(2022)))
    .unionByName(data2023.withColumn("year", F.lit(2023)))
    .unionByName(data2024.withColumn("year", F.lit(2024)))
)

cause_group_disruptions_per_year = (
    data_with_years
    .groupBy("year","cause_group", "statistical_cause_en")
    .count()
)

disruptions_all_years = (
    data_with_years
    .groupBy("cause_group", "statistical_cause_en")
    .count()
    .withColumn("year", F.lit("all_years"))
)

cause_group_disruptions_final = cause_group_disruptions_per_year.unionByName(disruptions_all_years)

cause_group_duration_by_year = (
    data_with_years
    .groupBy("year", "cause_group", "statistical_cause_en") 
    .agg(F.sum("duration_minutes").alias("total_minutes"))
)

all_years_duration = (
    data_with_years
    .groupBy("cause_group", "statistical_cause_en")
    .agg(F.sum("duration_minutes").alias("total_minutes"))
    .withColumn("year", F.lit("all_years"))
)

cause_group_duration_final = (
    cause_group_duration_by_year
    .unionByName(all_years_duration)
)

cause_group_disruptions_final \
    .coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(r"/user/s3063496/final_project/results/cause_group_disruptions_by_year")

cause_group_duration_final \
    .coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(r"/user/s3063496/final_project/results/cause_group_duration_by_year")
