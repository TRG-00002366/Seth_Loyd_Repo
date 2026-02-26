<<<<<<< HEAD
import string
import re
from pyspark import SparkContext

lines = sc.textFile("Mini_Project_1/sherlock_holmes.txt")

#T1
normal = normal.map(lambda line: re.sub(r'[^\w\s]', '', line.lower()))

#T2
tokenized = lines.flatmap(lambda x: x.split())

#T3
stopwords = set(["the","a","an","is","of","and","to","in","it"])
broadcast_stopwords = sc.broadcast(stopwords)
filtered_words = lines.filter(lambda w: w not in broadcast_stopwords.value)

#T4
total_chars = lines.map(lambda line: len(line)).reduce(lambda a, b: a + b)

#T5
longest_line = lines.map(lambda line: (len(line), line)).reduce(lambda a, b: a if a[0] > b[0] else b)
# longest_line[0] -> length, longest_line[1] -> text

#T6
watson_lines = lines.filter(lambda x: 'Watson' in x)

#T7


#T8


#T9


#T10


#T11


#T12




