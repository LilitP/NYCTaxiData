from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *

PROCENT_INTERVAL = 10
INPUT_YEAR = 2021
PATH_GREEN_TAXI_DATA = "/usr/local/spark/resources/data/Green/"
PATH_YELLOW_TAXI_DATA = "/usr/local/spark/resources/data/Yellow/"
PATH_TO_SAVE_OUTPUT_CSV = f"/usr/local/spark/resources/data/TipPercentage{INPUT_YEAR}"

spark = SparkSession.builder \
    .master("local") \
    .appName("NYCTaxiDataAnalysis") \
    .getOrCreate()

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


dfYellow = read_parquet(spark, PATH_YELLOW_TAXI_DATA)
dfGreen = read_parquet(spark, PATH_GREEN_TAXI_DATA)

dfCoreYellow = dfYellow.select("tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance",
                               "fare_amount", "tip_amount") \
    .withColumn("taxi_type", lit("Yellow")) \
    .withColumn("pickup_datetime", col("tpep_pickup_datetime")).withColumn("dropoff_datetime",
                                                                           col("tpep_dropoff_datetime")) \
    .drop("tpep_pickup_datetime", "tpep_dropoff_datetime")
dfCoreGreen = dfGreen.select("lpep_pickup_datetime", "lpep_dropoff_datetime", "passenger_count", "trip_distance",
                             "fare_amount", "tip_amount") \
    .withColumn("taxi_type", lit("Green")) \
    .withColumn("pickup_datetime", col("lpep_pickup_datetime")).withColumn("dropoff_datetime",
                                                                           col("lpep_dropoff_datetime")) \
    .drop("lpep_pickup_datetime", "lpep_dropoff_datetime")

dfAllTaxiRidesCleaned = cleanByTime(dfCoreYellow.union(dfCoreGreen), INPUT_YEAR)
dfAllTaxiRidesCleaned.show()
dfGreenAndYellow = do_agregations(add_id_column(dfAllTaxiRidesCleaned, "pickup_datetime"),
                                  dfAllTaxiRidesCleaned.count(), PROCENT_INTERVAL, INPUT_YEAR, "Yellow and Green")
dfGreenAndYellow.show()
saveToCsv(dfGreenAndYellow, PATH_TO_SAVE_OUTPUT_CSV + "Yellow and Green")

dfYellowTaxiRidesCleaned = cleanByTime(dfCoreYellow, INPUT_YEAR)
dfYellow = do_agregations(add_id_column(dfYellowTaxiRidesCleaned, "pickup_datetime"), dfYellowTaxiRidesCleaned.count(),
                          PROCENT_INTERVAL, INPUT_YEAR, "Yellow")

saveToCsv(dfYellow, PATH_TO_SAVE_OUTPUT_CSV + "Yellow")

dfGreenTaxiRidesCleaned = cleanByTime(dfCoreGreen, INPUT_YEAR)
dfGreen = do_agregations(add_id_column(dfGreenTaxiRidesCleaned, "pickup_datetime"), dfGreenTaxiRidesCleaned.count(),
                         PROCENT_INTERVAL, INPUT_YEAR, "Green")
dfGreen.show()
saveToCsv(dfGreen, PATH_TO_SAVE_OUTPUT_CSV + "Green")
