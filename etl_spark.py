    # 1) Medal tally per year & region (collect to driver and write CSV to avoid Hadoop native IO on Windows)
    medals_year_region = athlete_df.filter(F.col("is_medalist") == 1) \
        .groupBy("year", "region_name") \
        .agg(F.sum("gold").cast("int").alias("gold"),
             F.sum("silver").cast("int").alias("silver"),
             F.sum("bronze").cast("int").alias("bronze"))
    medals_year_region = medals_year_region.withColumn("total", F.col("gold") + F.col("silver") + F.col("bronze"))

    w = Window.partitionBy("year").orderBy(F.desc("total"), F.desc("gold"))
    medals_year_region = medals_year_region.withColumn("rank", F.row_number().over(w))

    out_medal_path = f"{out_path}/medal_tally_csv"
    LOG.info("Writing medal tally CSV to %s", out_medal_path)
    # create output folder if missing and write small aggregate to CSV via pandas to avoid native Hadoop calls
    Path(out_medal_path).mkdir(parents=True, exist_ok=True)
    try:
        medals_pd = medals_year_region.toPandas()
        medals_pd.to_csv(f"{out_medal_path}/medal_tally_all_years.csv", index=False)
    except Exception as e:
        LOG.warning("Failed to collect medal DataFrame to driver: %s", e)
        # fallback: attempt Spark CSV write (may still fail on Windows)
        try:
            medals_year_region.coalesce(1).write.mode("overwrite").option("header", "true").csv(out_medal_path)
        except Exception as e2:
            LOG.error("Failed to write medal tally via Spark CSV as well: %s", e2)

    # 2) Athlete career summary (collect small-ish summary to CSV)
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
    out_career_path = f"{out_path}/athlete_career_csv"
    LOG.info("Writing athlete career CSV to %s", out_career_path)
    Path(out_career_path).mkdir(parents=True, exist_ok=True)
    try:
        career.toPandas().to_csv(f"{out_career_path}/athlete_career_summary.csv", index=False)
    except Exception as e:
        LOG.warning("Could not collect career summary to pandas: %s", e)
        try:
            career.coalesce(1).write.mode("overwrite").option("header", "true").csv(out_career_path)
        except Exception as e2:
            LOG.error("Failed to write career summary via Spark CSV: %s", e2)

    # 3) Event participation & competitiveness (collect and write CSV)
    participants = athlete_df.groupBy("sport", "event", "year") \
        .agg(F.countDistinct("id").alias("num_athletes"),
             F.countDistinct("region_name").alias("num_countries"),
             F.sum("is_medalist").cast("int").alias("medals_awarded"))
    participants = participants.withColumn("medal_rate", F.round(F.col("medals_awarded") / F.col("num_athletes"), 4))
    out_event_path = f"{out_path}/event_participation_csv"
    LOG.info("Writing event participation CSV to %s", out_event_path)
    Path(out_event_path).mkdir(parents=True, exist_ok=True)
    try:
        participants.toPandas().to_csv(f"{out_event_path}/event_participation_by_year.csv", index=False)
    except Exception as e:
        LOG.warning("Could not collect event participation to pandas: %s", e)
        try:
            participants.coalesce(1).write.mode("overwrite").option("header", "true").csv(out_event_path)
        except Exception as e2:
            LOG.error("Failed to write event participation via Spark CSV: %s", e2)

    # Additional small outputs
    top_countries_all_time = medals_year_region.groupBy("region_name").agg(F.sum("total").alias("total_medals_all_time")) \
        .orderBy(F.desc("total_medals_all_time")).limit(100)
    out_top_path = f"{out_path}/top_countries_csv"
    Path(out_top_path).mkdir(parents=True, exist_ok=True)
    try:
        top_countries_all_time.toPandas().to_csv(f"{out_top_path}/top_countries_all_time.csv", index=False)
    except Exception as e:
        LOG.warning("Could not collect top countries: %s", e)
