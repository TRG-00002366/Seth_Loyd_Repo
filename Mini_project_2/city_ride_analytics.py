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
rides.groupBy("ride_type").agg(
    sum("fare_amount")
).show()

#T9
rides.groupBy("driver_id").agg(
    avg("rating")
).show()

#T10
joined = rides.join(drivers, rides.driver_id == drivers.driver_id)
joined.show()

#T11
peak = rides.filter(col("ride_date") >= "2025-01-01")
off_peak = rides.filter(col("ride_date") >= "2025-02-01")
df = peak.union(off_peak)
df.show()

#T12
rides.createOrReplaceTempView("ride_table")
result = spark.sql("SELECT fare_amount FROM ride_table ORDER BY fare_amount desc limit 3").show()
print(result)

#O1
sorted_rides_df = rides.orderBy(
    col("fare_amount").desc(),
    col("distance_miles").asc()
)

sorted_rides_df.show()

#O2
null_count = rides.filter(col("rating").isNull()).count()
print("Number of rides with null rating:", null_count)
rides = rides.fillna({"rating": 0.0})
rides.show()

#O3
rides = rides.withColumn(
    "ride_category",
    when(col("distance_miles") < 3, "short")
    .when((col("distance_miles") >= 3) & (col("distance_miles") <= 8), "medium")
    .otherwise("long")
)
rides.show()

#O4
enriched_df = rides.join(drivers, on="driver_id", how="inner")
window_spec = Window.partitionBy("driver_id").orderBy("ride_date")

enriched_df = enriched_df.withColumn(
    "running_revenue",
    sum("fare_amount").over(window_spec)
)
enriched_df.show()

#O5
enriched_df.write.mode("overwrite").parquet("Ride_Analytics_Results/enriched_rides")
driver_revenue_df.write.mode("overwrite").option("header", True).csv("Ride_Analytics_Results/driver_revenue")

spark.stop()