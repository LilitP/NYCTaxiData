from pyspark.sql.window import Window
from pyspark.sql.functions import *


def read_parquet(spark, path: str):
    return spark.read.parquet(path)


def saveToCsv(df: DataFrame, path: str):
    df.coalesce(1) \
        .write.format('com.databricks.spark.csv') \
        .save(path, header='true')


def cleanByTime(df: DataFrame, year: int) -> DataFrame:
    date = f"{year}-01-01 00:00:00"
    return df.filter(col("pickup_datetime") >= to_timestamp(lit(date), "yyyy-MM-dd HH:mm:ss"))


def add_id_column(df: DataFrame, column: str) -> DataFrame:
    w = Window().orderBy(column)
    return df.select(row_number().over(w).alias("id"), col("*"))


def do_agregations(df: DataFrame, tripsCount: int, interval: int, input_year: int, taxiType: str) -> DataFrame:
    dfWithRanges = df.select("id", "fare_amount", "tip_amount").withColumn("tipRange", (
            col("tip_amount") / col("fare_amount") * 100)) \
        .withColumn("tipRangeInt", col("tipRange").cast('int').alias("tipRangeInt"))

    dfWithCalculations = dfWithRanges.withColumn("TipProcent", when(col("tipRange").isNull(), 1000)
                                                 .otherwise(when((col("tipRangeInt") < 50) & (col("tipRangeInt") >= 0),
                                                                 col("tipRangeInt") - (col("tipRangeInt") % interval))
        .otherwise(
        when(col("tipRangeInt") >= 50, 50).otherwise(1000)))) \
        .withColumn("TipProcent", when(col("TipProcent") == 1000, "Error")
                    .otherwise(
        when(col("TipProcent") < 50, concat(col("TipProcent"), lit(" - "), (col("TipProcent") + interval)))
            .otherwise(when(col("TipProcent") >= 50, "50+")))) \
        .groupBy("TipProcent") \
        .agg(count(col("id")) * 100 / tripsCount) \
        .withColumn(f"Percentage of all {taxiType} Taxi Rides in {input_year} year in NYC",
                    col(f"((count(id) * 100) / {tripsCount})")) \
        .drop(col(f"((count(id) * 100) / {tripsCount})")) \
        .orderBy(col("TipProcent"))
    return dfWithCalculations
