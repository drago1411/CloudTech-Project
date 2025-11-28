#!/usr/bin/env python3
"""
small_etl.py
Lightweight PySpark ETL for Olympics dataset (Windows-friendly).

Reads:
  - data/athlete_events.csv
  - data/noc_regions.csv

Writes:
  - output/medal_tally_all_years.csv
  - output/athlete_career_summary.csv
  - output/event_participation_by_year.csv
  - output/top_countries_all_time.csv
  - output/gender_participation_trends.csv
  - output/dominant_sport_per_country.csv

This script uses Spark for distributed reading + aggregation, then
collects aggregated results to pandas and writes CSV files locally (avoids native Hadoop I/O).
"""

import os
from pathlib import Path
import logging

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("small_etl")


def create_spark():
    spark = SparkSession.builder.appName("OlympicsSmallETL").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    return spark


def safe_cast_numeric(df, colname):
    if colname in df.columns:
        return df.withColumn(colname, F.when(F.col(colname).rlike(r"^-?\d+(\.\d+)?$"), F.col(colname).cast(T.DoubleType())).otherwise(None))
    return df


def main():
    base = Path.cwd()
    data_dir = base / "data"
    out_dir = base / "output"
    out_dir.mkdir(parents=True, exist_ok=True)

    athlete_csv = str(data_dir / "athlete_events.csv")
    noc_csv = str(data_dir / "noc_regions.csv")

    spark = create_spark()
    logger.info("Reading athlete CSV: %s", athlete_csv)
    athlete_df = spark.read.options(header="true", multiLine="true", escape='"', inferSchema="true").csv(athlete_csv)
    logger.info("Reading noc CSV: %s", noc_csv)
    noc_df = spark.read.options(header="true", inferSchema="true").csv(noc_csv)

    
    athlete_df = athlete_df.select([F.col(c).alias(c.lower()) for c in athlete_df.columns])
    noc_df = noc_df.select([F.col(c).alias(c.lower()) for c in noc_df.columns])

    
    for c in ["name", "team", "sport", "event", "games", "noc", "city", "season"]:
        if c in athlete_df.columns:
            athlete_df = athlete_df.withColumn(c, F.trim(F.col(c)))

    
    if "id" in athlete_df.columns:
        athlete_df = athlete_df.withColumn("id", F.col("id").cast(T.IntegerType()))
    if "year" in athlete_df.columns:
        athlete_df = athlete_df.withColumn("year", F.col("year").cast(T.IntegerType()))

    athlete_df = safe_cast_numeric(athlete_df, "age")
    athlete_df = safe_cast_numeric(athlete_df, "height")
    athlete_df = safe_cast_numeric(athlete_df, "weight")

    
    if "region" in noc_df.columns:
        noc_df = noc_df.withColumnRenamed("region", "region_name")
    elif "region_name" not in noc_df.columns:
        noc_df = noc_df.withColumn("region_name", F.col("noc"))
    if "noc" not in noc_df.columns:
        noc_df = noc_df.withColumn("noc", F.col(noc_df.columns[0]))

    athlete_df = athlete_df.join(noc_df.select("noc", "region_name"), on="noc", how="left")
    athlete_df = athlete_df.withColumn("region_name", F.coalesce(F.col("region_name"), F.col("team")))

    
    if "medal" in athlete_df.columns:
        athlete_df = athlete_df.withColumn("medal", F.when(F.col("medal").isNull(), F.lit("None")).otherwise(F.col("medal")))
    else:
        athlete_df = athlete_df.withColumn("medal", F.lit("None"))

    athlete_df = athlete_df.withColumn("is_medalist", F.when(F.col("medal") != "None", 1).otherwise(0))
    athlete_df = athlete_df.withColumn("gold", F.when(F.col("medal") == "Gold", 1).otherwise(0))
    athlete_df = athlete_df.withColumn("silver", F.when(F.col("medal") == "Silver", 1).otherwise(0))
    athlete_df = athlete_df.withColumn("bronze", F.when(F.col("medal") == "Bronze", 1).otherwise(0))

    logger.info("Cleaned athlete records count: %d", athlete_df.count())



    # 1 - Medal tally by year & region

    medals = athlete_df.filter(F.col("is_medalist") == 1) \
        .groupBy("year", "region_name") \
        .agg(F.sum("gold").cast("int").alias("gold"),
             F.sum("silver").cast("int").alias("silver"),
             F.sum("bronze").cast("int").alias("bronze"))
    medals = medals.withColumn("total", F.col("gold") + F.col("silver") + F.col("bronze"))
    w = Window.partitionBy("year").orderBy(F.desc("total"), F.desc("gold"))
    medals = medals.withColumn("rank", F.row_number().over(w))
    logger.info("Collecting medal tally to driver (may take a moment)...")
    medals_pd = medals.toPandas()
    medals_pd.to_csv(out_dir / "medal_tally_all_years.csv", index=False)
    logger.info("Wrote medal_tally_all_years.csv (%d rows)", len(medals_pd))


    # 2 - Athlete career summary

    agg_exprs = []
    if "is_medalist" in athlete_df.columns:
        agg_exprs += [F.sum("is_medalist").cast("int").alias("total_medals")]
    if "gold" in athlete_df.columns:
        agg_exprs += [F.sum("gold").cast("int").alias("gold"),
                      F.sum("silver").cast("int").alias("silver"),
                      F.sum("bronze").cast("int").alias("bronze")]
    if "age" in athlete_df.columns:
        agg_exprs += [F.round(F.avg("age"), 2).alias("avg_age_at_comp")]
    agg_exprs += [F.min("year").alias("first_year"), F.max("year").alias("last_year"),
                  F.countDistinct("year").alias("num_appearances")]

    career = athlete_df.groupBy("id", "name", "sex").agg(*agg_exprs)
    logger.info("Collecting athlete career summary to driver...")
    career.toPandas().to_csv(out_dir / "athlete_career_summary.csv", index=False)
    logger.info("Wrote athlete_career_summary.csv")



    # 3 - Event participation & competitiveness


    participants = athlete_df.groupBy("sport", "event", "year") \
        .agg(F.countDistinct("id").alias("num_athletes"),
             F.countDistinct("region_name").alias("num_countries"),
             F.sum("is_medalist").cast("int").alias("medals_awarded"))
    participants = participants.withColumn("medal_rate", F.round(F.col("medals_awarded") / F.col("num_athletes"), 4))
    logger.info("Collecting event participation to driver...")
    participants.toPandas().to_csv(out_dir / "event_participation_by_year.csv", index=False)
    logger.info("Wrote event_participation_by_year.csv")



    # 4 - Top countries all-time

    top_countries = medals.groupby("region_name").agg(F.sum("total").alias("total_medals_all_time")) \
        .orderBy(F.desc("total_medals_all_time"))
    top_countries.toPandas().head(100).to_csv(out_dir / "top_countries_all_time.csv", index=False)
    logger.info("Wrote top_countries_all_time.csv")



    # 5 - Gender Participation Trends
    
    logger.info("Computing gender participation trends...")

    # count distinct athletes by year and sex
    gender_trends = (
        athlete_df.groupBy("year", "sex")
                  .agg(F.countDistinct("id").alias("athlete_count"))
                  .orderBy("year")
    )

    gender_df = gender_trends.toPandas()
    gender_path = out_dir / "gender_participation_trends.csv"
    gender_df.to_csv(gender_path, index=False)
    logger.info(f"Wrote {gender_path.name} ({len(gender_df)} rows)")



    # 6 - Dominant Sport for Each Country

    logger.info("Computing dominant sport for each country...")

    dominant_sports = (
        athlete_df.groupBy("region_name", "sport")
                  .agg(F.sum("is_medalist").alias("total_medals"))
    )

    window_dom = Window.partitionBy("region_name").orderBy(F.desc("total_medals"))

    dominant_sports_ranked = dominant_sports.withColumn(
        "rank", F.row_number().over(window_dom)
    )

    top_sport = dominant_sports_ranked.filter(
        F.col("rank") == 1
    ).select("region_name", "sport", "total_medals")

    dom_df = top_sport.toPandas()
    dom_path = out_dir / "dominant_sport_per_country.csv"
    dom_df.to_csv(dom_path, index=False)
    logger.info(f"Wrote {dom_path.name} ({len(dom_df)} rows)")
    spark.stop()
    logger.info("All done. Outputs are in: %s", str(out_dir.resolve()))


if __name__ == "__main__":
    main()
