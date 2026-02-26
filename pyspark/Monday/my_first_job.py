from pyspark.SparkSession import SparkSession
#covers both task 1 and 2 I belive
def main():
    # Step 1: Create SparkSession
    spark = SparkSession.builder \
        .appName("MyFirstJob") \
        .master("local[*]") \
        .getOrCreate()
    
    # Step 2: YOUR CODE HERE - Create some data
    data = [1,2,3,4,5]
    sales_data = [
    ("Laptop", "Electronics", 999.99, 5),
    ("Mouse", "Electronics", 29.99, 50),
    ("Desk", "Furniture", 199.99, 10),
    ("Chair", "Furniture", 149.99, 20),
    ("Monitor", "Electronics", 299.99, 15),
]
    # Step 3: YOUR CODE HERE - Perform transformations
    df = spark.createDataFrame(sales_data, ['product', 'category', 'price', 'quantity'])
    rdd = spark.parallelize(data)
    # Step 4: YOUR CODE HERE - Show results
    print(rdd.collect())
    df.show()
    # Step 5: Clean up
    spark.stop()

if __name__ == "__main__":
    main()
