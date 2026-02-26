def main():
    from pyspark import SparkContext

    sc = SparkContext("local[*]", "AccumulatorExercise")

    # Create accumulator
    record_counter = sc.accumulator(0)

    # Sample data
    data = sc.parallelize(range(1, 101))

    # Count records using accumulator
    def count_record(x):
        record_counter.add(1)
        return x

    data.map(count_record).collect()

    print(f"Records processed: {record_counter.value}")
    # Expected: 100



if __name__ == "__main__":
    main()
