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