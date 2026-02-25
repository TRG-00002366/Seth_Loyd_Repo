from pyspark import SparkContext

sc = SparkContext("local[*]", "RDDActions")

numbers = sc.parallelize([10, 5, 8, 3, 15, 12, 7, 20, 1, 9])

# Task A: collect() - Get all elements
all_nums = # YOUR CODE
print(f"All numbers: {all_nums}")

# Task B: count() - Count elements
count = # YOUR CODE
print(f"Count: {count}")

# Task C: first() - Get first element
first = # YOUR CODE
print(f"First: {first}")

# Task D: take(n) - Get first n elements
first_three = # YOUR CODE
print(f"First 3: {first_three}")

# Task E: top(n) - Get largest n elements
top_three = # YOUR CODE
print(f"Top 3: {top_three}")

# Task F: takeOrdered(n) - Get smallest n elements
smallest_three = # YOUR CODE
print(f"Smallest 3: {smallest_three}")

sc.stop()