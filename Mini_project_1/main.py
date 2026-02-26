<<<<<<< HEAD
import string
import re
from pyspark import SparkContext

lines = sc.textFile("Mini_Project_1/sherlock_holmes.txt")

#T1
normal = lines.lower()
normal = normal.map(remove_punctuation_rdd)

#T2
tokenized = lines.flatmap(lambda x: x.split())

#T3
filtered = tokenized.filter(lambda x: x != "the" or x != "a" or x != "an" or x != "is")

def remove_punctuation_rdd(text_line):
    # Use regex to substitute any character that is NOT a word character (\w) or whitespace (\s)
    # with an empty string. This effectively removes all punctuation.
    clean_line = re.sub(r'[^\\w\\s]', '', text_line)
    # Optional: convert to lowercase and strip leading/trailing spaces
    return clean_line.lower().strip()
=======
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
>>>>>>> 45dffacd18d7a780bf1f2d8418c97baec586bf33
