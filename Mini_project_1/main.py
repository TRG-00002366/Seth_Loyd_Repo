from operator import add
import string
import re
from pyspark import SparkContext

sc = SparkContext(["local[*]", "HolmesAnalisis"])

lines_rdd = sc.textFile("sherlock_holmes.txt")

#T1
normalized_rdd = lines_rdd.map(lambda line: line.translate(str.maketrans('', '', string.punctuation)).lower())

#T2
tokenized = lines_rdd.flatMap(lambda x: x.split())

#T3
stopwords = set(["the","a","an","is","of","and","to","in","it"])
broadcast_stopwords = sc.broadcast(stopwords)
filtered_words_rdd = tokenized.filter(lambda w: w not in broadcast_stopwords.value)

#T4
total_chars = lines_rdd.map(lambda line: len(line)).reduce(lambda a, b: a + b)

#T5
longest_line = lines_rdd.map(lambda line: (len(line), line)).reduce(lambda a, b: a if a[0] > b[0] else b)
# longest_line[0] -> length, longest_line[1] -> text

#T6
watson_lines = lines_rdd.filter(lambda x: 'Watson' in x)

#T7
unique_words_count = filtered_words_rdd.distinct().count()
print("Unique words:", unique_words_count)

#T8
word_counts = filtered_words_rdd.map(lambda word: (word, 1)).reduceByKey(add)
top_10_words = word_counts.takeOrdered(10, key=lambda x: -x[1])
print(top_10_words)

#T9
first_words_rdd = normalized_rdd.map(lambda line: line.split()[0] if line else "").filter(lambda w: w != "")
start_word_counts = first_words_rdd.map(lambda w: (w, 1)).reduceByKey(add)
print(start_word_counts.take(10))

#T10
total_word_length = filtered_words_rdd.map(lambda w: len(w)).reduce(add)
total_word_count = filtered_words_rdd.count()
avg_word_length = total_word_length / total_word_count
print("Average word length:", avg_word_length)

#T11
word_length_dist = filtered_words_rdd.map(lambda w: (len(w), 1)).reduceByKey(add).sortByKey()
print(word_length_dist.collect())

#T12
chapter_lines_rdd = normalized_rdd.zipWithIndex()  # keep index for ordering
chapter_start = chapter_lines_rdd.filter(lambda x: "a scandal in bohemia" in x[0]).first()[1]
chapter_end = chapter_lines_rdd.filter(lambda x: "the red-headed league" in x[0]).first()[1]

specific_chapter_rdd = chapter_lines_rdd.filter(lambda x: chapter_start <= x[1] < chapter_end).map(lambda x: x[0])

#O1
filtered_words_rdd_broadcast = tokenized.filter(lambda w: w not in broadcast_stopwords.value)

#O2
blank_line_acc = sc.accumulator(0)

def count_blank_lines(line):
    if not line.strip():
        blank_line_acc.add(1)
    return line

_ = normalized_rdd.map(count_blank_lines).collect()
print("Blank lines:", blank_line_acc.value)

#O3
word_length_pair_rdd = filtered_words_rdd.map(lambda w: (len(w), w))
print(word_length_pair_rdd.take(5))

#O4
char_counts_rdd = normalized_rdd.flatMap(lambda line: list(line.replace(" ", ""))) \
    .map(lambda c: (c, 1)).reduceByKey(add)
print(char_counts_rdd.take(10))

#O5
grouped_words_rdd = filtered_words_rdd.map(lambda w: (w[0], w)).groupByKey().mapValues(list)
print(grouped_words_rdd.take(5))

#O6
import os

file_path = "sherlock_holmes.txt"
if os.path.exists(file_path):
    lines_rdd = sc.textFile(file_path)
else:
    print(f"Error: File {file_path} not found")

#O7
word_counts.saveAsTextFile("Holmes_WordCount_Results")

sc.stop()
