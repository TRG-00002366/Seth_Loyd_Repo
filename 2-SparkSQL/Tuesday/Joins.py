"""
Exercise: Joins
===============
Week 2, Tuesday

Practice all join types with customer and order data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast

# =============================================================================
# SETUP - Do not modify
# =============================================================================

spark = SparkSession.builder.appName("Exercise: Joins").master("local[*]").getOrCreate()

# Customers
customers = spark.createDataFrame([
    (1, "Alice", "alice@email.com", "NY"),
    (2, "Bob", "bob@email.com", "CA"),
    (3, "Charlie", "charlie@email.com", "TX"),
    (4, "Diana", "diana@email.com", "FL"),
    (5, "Eve", "eve@email.com", "WA")
], ["customer_id", "name", "email", "state"])

# Orders
orders = spark.createDataFrame([
    (101, 1, "2023-01-15", 150.00),
    (102, 2, "2023-01-16", 200.00),
    (103, 1, "2023-01-17", 75.00),
    (104, 3, "2023-01-18", 300.00),
    (105, 6, "2023-01-19", 125.00),  # customer_id 6 does not exist!
    (106, 2, "2023-01-20", 180.00)
], ["order_id", "customer_id", "order_date", "amount"])

# Products (for multi-table join)
products = spark.createDataFrame([
    (101, "Laptop"),
    (102, "Phone"),
    (103, "Mouse"),
    (104, "Keyboard"),
    (107, "Monitor")  # Not in any order!
], ["order_id", "product_name"])

print("=== Exercise: Joins ===")
print("\nCustomers:")
customers.show()
print("Orders:")
orders.show()
print("Products:")
products.show()

# =============================================================================
# TASK 1: Inner Join (15 mins)
# =============================================================================

print("\n--- Task 1: Inner Join ---")

# TODO 1a: Join customers and orders (only matching records)
# Show customer name, order_id, order_date, amount
results = customers.join(orders, customers.customer_id == orders.customer_id)
results.select("name","order_id","order_date","amount").show()

# TODO 1b: How many orders have matching customers?
# HINT: Compare this count to total orders
match = orders.count() - results.count()
print(f"{match} orders have matching customers")

# =============================================================================
# TASK 2: Left and Right Joins (20 mins)
# =============================================================================

print("\n--- Task 2: Left and Right Joins ---")

# TODO 2a: LEFT JOIN - All customers, with order info where available
# Who has NOT placed any orders?
left_result = customers.join(orders, customers.customer_id == orders.customer_id, "left")
left_result.select("name").filter(col("order_id").isNull).show()

# TODO 2b: RIGHT JOIN - All orders, with customer info where available
# Which order has no matching customer?
right_results = customers.join(orders, customers.customer_id == orders.customer_id, "right")
right_results.select("order_id").filter(col("customer_id").isNull).show()

# TODO 2c: What is the difference between the two results?
# Answer in a comment:
#the left join returns customer id while the right join returns the order id
#

# =============================================================================
# TASK 3: Full Outer Join (10 mins)
# =============================================================================

print("\n--- Task 3: Full Outer Join ---")

# TODO 3a: Perform a FULL OUTER join between customers and orders
# All customers AND all orders should appear
results = customers.join(orders, customers.customer_id == orders.customer_id, "outer").show()
results.show()
# TODO 3b: Filter to show only rows where there is a mismatch
# (customer without order OR order without customer)
mismatches = results.filter(
    col("order_id").isNull() | col("name").isNull()
)
mismatches.show()

# =============================================================================
# TASK 4: Semi and Anti Joins (15 mins)
# =============================================================================

print("\n--- Task 4: Semi and Anti Joins ---")

# TODO 4a: LEFT SEMI JOIN - Customers who HAVE placed orders
# Only customer columns should appear
customers_with_orders = customers.join(orders, customer.customer_id== orders.customer_id, "left_semi")
customers_with_orders.show()

# TODO 4b: LEFT ANTI JOIN - Customers who have NOT placed orders
customers_no_orders = customers.join(orders, customer.customer_id == orders.customer_id, "left_anti")
customers_no_orders.show()
# TODO 4c: When would you use anti join in real data work?
# Answer in a comment:
#Data quality validation and referential integrity checks
#and detecting failed ETL loads

# =============================================================================
# TASK 5: Handling Duplicate Columns (15 mins)
# =============================================================================

print("\n--- Task 5: Handling Duplicate Columns ---")

# After joining customers and orders, both have customer_id

# TODO 5a: Join and then DROP the duplicate customer_id column
joined_df = customers.join(
    orders,
    customers.customer_id == orders.customer_id,
    "inner"
)

clean_df = joined_df.drop(orders.customer_id)
clean_df.show()

# TODO 5b: Alternative: Use aliases to reference specific columns
# HINT: customers.alias("c"), orders.alias("o")
c = customers.alias("c")
o = orders.alias("o")

joined_alias = c.join(
    o,
    col("c.customer_id") == col("o.customer_id"),
    "inner"
)

result = joined_alias.select(
    col("c.customer_id"),
    col("c.name"),
    col("o.order_id"),
    col("o.amount")
)

result.show()

# =============================================================================
# TASK 6: Multi-Table Join (15 mins)
# =============================================================================

print("\n--- Task 6: Multi-Table Join ---")

# TODO 6a: Join customers -> orders -> products
# Show: customer name, order_id, amount, product_name
from pyspark.sql.functions import col

c = customers.alias("c")
o = orders.alias("o")
p = products.alias("p")

multi_join = (
    c.join(o, col("c.customer_id") == col("o.customer_id"), "inner")
     .join(p, col("o.order_id") == col("p.order_id"), "inner")
)

result = multi_join.select(
    col("c.name"),
    col("o.order_id"),
    col("o.amount"),
    col("p.product_name")
)

result.show()

# TODO 6b: What kind of join should you use when some orders might not have products?
#Left join

# =============================================================================
# CHALLENGE: Real-World Scenarios (20 mins)
# =============================================================================

print("\n--- Challenge: Real-World Scenarios ---")

# TODO 7a: Find the total spending per customer (only customers with orders)
# Use join + groupBy + sum
total_spend = (
    customers.join(orders, on="customer_id", how="inner")
             .groupBy("customer_id", "name")
             .agg(sum("amount").alias("total_spent"))
)

total_spend.show()

# TODO 7b: Find customers from CA who placed orders > $150
ca_high_spenders = (
    customers.filter(col("state") == "CA")
             .join(orders, on="customer_id", how="inner")
             .filter(col("amount") > 150)
             .select("name", "order_id", "amount")
)

ca_high_spenders.show()

# TODO 7c: Find orders without valid product information
# (anti join pattern)
orders_without_products = orders.join(
    products,
    on="order_id",
    how="left_anti"
)

orders_without_products.show()

# =============================================================================
# CLEANUP
# =============================================================================

spark.stop()
