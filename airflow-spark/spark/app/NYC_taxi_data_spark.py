from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *

PROCENT_INTERVAL = 10
INPUT_YEAR = 2021
PATH_GREEN_TAXI_DATA ="/usr/local/spark/resources/data/Green/"
PATH_YELLOW_TAXI_DATA ="/usr/local/spark/resources/data/Yellow/"
PATH_TO_SAVE_OUTPUT_CSV =f"/usr/local/spark/resources/data/TipPercentage{INPUT_YEAR}"

# I was trying different configs because had a memory error in docker container and left as this
spark = SparkSession.builder \
    .master("local[3]") \
    .appName("NYCTaxiDataAnalysis") \
    .getOrCreate()

def read_parquet(spark, path: str) -> DataFrame:
    return spark.read.parquet(path)

def save_to_csv(df: DataFrame, path: str):
    df.coalesce(1) \
        .write.format('com.databricks.spark.csv') \
        .save(path, header='true')

# Looks like it's more flexible would be use just datatime ??
# My column is timestamp so I need to convert it anyway
def filter_by_time(df: DataFrame, year: int) -> DataFrame:
    date = f"{year}-01-01 00:00:00"
    return df.filter(col("pickup_datetime") >= to_timestamp(lit(date), "yyyy-MM-dd HH:mm:ss"))

def add_id_column(df: DataFrame) -> DataFrame:
    dfWithId = df.withColumn("monotonically_increasing_id", monotonically_increasing_id())
    w = Window.orderBy(col('monotonically_increasing_id'))
    return dfWithId.select(row_number().over(w).alias("id"), col("*")).drop("monotonically_increasing_id")

def do_agregations(df: DataFrame, tripsCount: int, interval: int, input_year: int, taxiType: str) -> DataFrame:
    dfWithRanges = df.select("id", "fare_amount", "tip_amount").withColumn("tipRange", (
            col("tip_amount") / col("fare_amount") * 100)) \
        .withColumn("tipRangeInt", col("tipRange").cast('int').alias("tipRangeInt"))

    dfWithCalculations = dfWithRanges \
        .withColumn("TipProcent", when(col("tipRange").isNull(), 1000) \
                    .otherwise(when((col("tipRangeInt") < 50) & (col("tipRangeInt") >= 0), col("tipRangeInt") - (col("tipRangeInt") % interval)) \
                               .otherwise(when(col("tipRangeInt") >= 50, 50) \
                                          .otherwise(1000)))) \
        .withColumn("TipProcent", when(col("TipProcent") == 1000, "Error") \
                    .otherwise(when(col("TipProcent") < 50, concat(col("TipProcent"), lit(" - "), (col("TipProcent") + interval))) \
                               .otherwise(when(col("TipProcent") >= 50, "50+")))) \
        .groupBy("TipProcent") \
        .agg(count(col("id")) * 100 / tripsCount) \
        .withColumn(f"Percentage of all {taxiType} Taxi Rides in {input_year} year in NYC",col(f"((count(id) * 100) / {tripsCount})")) \
        .drop(col(f"((count(id) * 100) / {tripsCount})")) \
        .orderBy(col("TipProcent"))

    # Does it make sense to put Error cases in result dataset?
    # Just to be sure there are not too much error cases
    return dfWithCalculations

dfYellow = read_parquet(spark, PATH_YELLOW_TAXI_DATA)
dfGreen = read_parquet(spark, PATH_GREEN_TAXI_DATA)

dfCoreYellow = dfYellow.select("tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance",
                               "fare_amount", "tip_amount") \
    .withColumn("taxi_type", lit("Yellow")) \
    .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
    .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
dfCoreGreen = dfGreen.select("lpep_pickup_datetime", "lpep_dropoff_datetime", "passenger_count", "trip_distance",
                             "fare_amount", "tip_amount") \
    .withColumn("taxi_type", lit("Green")) \
    .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime") \
    .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")

dfAllTaxiRidesCleaned = filter_by_time(dfCoreYellow.union(dfCoreGreen), INPUT_YEAR)
dfAllTaxiRidesCleaned.show()
dfGreenAndYellow = do_agregations(add_id_column(dfAllTaxiRidesCleaned),
                                  dfAllTaxiRidesCleaned.count(), PROCENT_INTERVAL, INPUT_YEAR, "Yellow and Green")
dfGreenAndYellow.show()
save_to_csv(dfGreenAndYellow, PATH_TO_SAVE_OUTPUT_CSV + "Yellow and Green")
dfYellowTaxiRidesCleaned = filter_by_time(dfCoreYellow, INPUT_YEAR)
dfYellow = do_agregations(add_id_column(dfYellowTaxiRidesCleaned), dfYellowTaxiRidesCleaned.count(),
                          PROCENT_INTERVAL, INPUT_YEAR, "Yellow")
dfYellow.show()
save_to_csv(dfYellow, PATH_TO_SAVE_OUTPUT_CSV + "Yellow")
dfGreenTaxiRidesCleaned = filter_by_time(dfCoreGreen, INPUT_YEAR)
dfGreen = do_agregations(add_id_column(dfGreenTaxiRidesCleaned), dfGreenTaxiRidesCleaned.count(),
                         PROCENT_INTERVAL, INPUT_YEAR, "Green")
dfGreen.show()
save_to_csv(dfGreen, PATH_TO_SAVE_OUTPUT_CSV + "Green")