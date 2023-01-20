from pyspark.sql import SparkSession
from app.agregation_functions import *

PROCENT_INTERVAL = 10
INPUT_YEAR = 2021
PATH_GREEN_TAXI_DATA = "data\Green"
PATH_YELLOW_TAXI_DATA = "data\Yellow"
PATH_TO_SAVE_OUTPUT_CSV = f"results\\TipPercentage{INPUT_YEAR}"

spark = SparkSession.builder \
    .master("local") \
    .appName("NYCTaxiDataAnalysis") \
    .getOrCreate()

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
dfYellow.show()
saveToCsv(dfYellow, PATH_TO_SAVE_OUTPUT_CSV + "Yellow")

dfGreenTaxiRidesCleaned = cleanByTime(dfCoreGreen, INPUT_YEAR)
dfGreen = do_agregations(add_id_column(dfGreenTaxiRidesCleaned, "pickup_datetime"), dfGreenTaxiRidesCleaned.count(),
                         PROCENT_INTERVAL, INPUT_YEAR, "Green")
dfGreen.show()
saveToCsv(dfGreen, PATH_TO_SAVE_OUTPUT_CSV + "Green")
