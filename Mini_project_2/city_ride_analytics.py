from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("City Ride Analytics").master("local[*]").getOrCreate()

#T1
drivers = spark.read.csv("drivers.csv", header = True, inferSchema = True)
rides = spark.read.csv("rides.csv", header = True, inferSchema = True)

#T2
drivers.printSchema()
drivers.show(5)
rides.printSchema()
rides.show(5)

#T3
rides.select("ride_id","pickup_location","dropoff_location","fare_amount").show()

#T4
rides.filter(col("distance_miles")> 5.0 & col("ride_type") == "premium").show()

#T5
rides.withColumn("fare_per_mile", col("fare_amount")/col("distance_miles")).show()

#T6
rides.drop("ride_type").show()

#T7
rides.withColumnRenamed("pickup_location","start_area")\
    .withColumnRenamed("dropoff_location","end_area")\
    .show()

#T8


spark.stop()