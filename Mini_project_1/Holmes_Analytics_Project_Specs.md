# Project Spec: Sherlock Holmes Analytics (PySpark RDD Edition)

## 1. Project Overview
In this project, you will act as a Data Engineer for the "Baker Street Data Agency." Your task is to analyze the classic text **"The Adventures of Sherlock Holmes"** using PySpark's RDD API. You will demonstrate your mastery of RDD transformations, actions, shared variables, and Pair RDDs to extract insights from the text.

---

## 2. Learning Objectives
By the end of this project, students will be able to:
- Load and save data using Spark RDDs.
- Apply basic and advanced transformations (`map`, `filter`, `flatMap`, `reduceByKey`, etc.).
- Execute actions to retrieve results (`collect`, `count`, `take`, etc.).
- Utilize Shared Variables (`Broadcast` and `Accumulators`) for optimization and global metrics.
- Perform complex data manipulation using `Pair RDDs`.

---

## 3. Data Source
- **Input File:** `sherlock_holmes.txt` (The Adventures of Sherlock Holmes by Arthur Conan Doyle).
- **Format:** Plain text.

---

## 4. Requirements: Transformations & Actions (12 Tasks)

### T1: Text Normalization
**Requirement:** Convert all text to lowercase and remove punctuation to ensure uniform analysis.
- **Example:** If the line is "Elementary, my dear Watson!", it should become "elementary my dear watson".

### T2: Word Tokenization
**Requirement:** Split the text into individual words.
- **Example:** A sentence like "I am a consulting detective" becomes a list: `["I", "am", "a", "consulting", "detective"]`.

### T3: Filtering Stopwords
**Requirement:** Filter out common words (e.g., "the", "a", "an", "is") that don't add much value to the analysis.
- **Example:** "The secret of the case" becomes "secret case".

### T4: Character Counting
**Requirement:** Calculate the total number of characters in the entire book.
- **Example:** Like counting every single letter and space in the physical book.

### T5: Line Length Analysis
**Requirement:** Find the longest line in the text and its character count.
- **Example:** Identifying the sentence where Sherlock gives his longest uninterrupted deduction.

### T6: Word Search (Filtering)
**Requirement:** Extract all lines that contain the word "Watson".
- **Example:** Like using 'Ctrl+F' to highlight every time Holmes addresses his assistant.

### T7: Unique Vocabulary Count
**Requirement:** Count how many unique words are used throughout the book.
- **Example:** Determining if Doyle used 5,000 or 10,000 different words to write the story.

### T8: Top 10 Frequent Words
**Requirement:** Identify the 10 most frequently used words in the book.
- **Example:** Finding out if "Sherlock" appears more often than "Holmes".

### T9: Sentence Start Distribution
**Requirement:** Find the first word of every line and count their occurrences.
- **Example:** Seeing how many times a description starts with the word "It" or "He".

### T10: Average Word Length
**Requirement:** Compute the average length of all words in the text.
- **Example:** Determining if the vocabulary is mostly made of short words (like "run") or long words (like "circumstantial").

### T11: Distribution of Word Lengths
**Requirement:** Count how many words have 1 letter, 2 letters, 3 letters, etc.
- **Example:** A tally chart showing "3-letter words: 500, 4-letter words: 800".

### T12: Specific Chapter Extraction
**Requirement:** Filter and collect text only between "A SCANDAL IN BOHEMIA" and "THE RED-HEADED LEAGUE".
- **Example:** Ripping out just the pages related to the first story in the collection.

---

## 5. Requirements: Shared Variables, Pair RDDs, & I/O (7 Tasks)

### O1: Efficient Stopword Filtering (Broadcast)
**Requirement:** Use a **Broadcast Variable** to store the list of stopwords for efficient filtering across all worker nodes.
- **Example:** Giving every student in a classroom a copy of the "Banned Words" list instead of shouting the list every time someone writes a sentence.

### O2: Global Error/Blank Line Counter (Accumulator)
**Requirement:** Use an **Accumulator** to count how many blank lines or empty strings are encountered during processing.
- **Example:** Having a central scoreboard that ticks up every time a worker finds a blank page in the book.

### O3: Word-Length Pairing (Pair RDD)
**Requirement:** Map each word to a pair consisting of the word length and the word itself.
- **Example:** Labeling the word "Case" as `(4, "Case")` and "Sherlock" as `(8, "Sherlock")`.

### O4: Character Frequency (Pair RDD / ReduceByKey)
**Requirement:** Calculate the frequency of every letter in the alphabet using a Pair RDD.
- **Example:** A final list showing `(a, 1200), (b, 400), (c, 800)...`

### O5: Grouped Word Lists (GroupByKey)
**Requirement:** Group all words that start with the same letter together.
- **Example:** A folder labeled 'S' containing `["Sherlock", "Secret", "Scandal", "Study"]`.

### O6: Loading from Multiple Sources
**Requirement:** Configure the Spark program to load the text file from a local path and handle missing file errors gracefully.
- **Example:** Checking if the book is on the shelf before trying to read it.

### O7: Saving Analytics Results
**Requirement:** Save the word count results as a text file in a directory named `Holmes_WordCount_Results`.
- **Example:** Writing the final report and filing it into a specific drawer for the client.

---

## 6. Evaluation Criteria
- **Correctness:** Does the logic correctly address the Sherlock Holmes dataset?
- **Efficiency:** Are Broadcast variables used where appropriate?
- **RDD Best Practices:** Is `reduceByKey` preferred over `groupByKey` for aggregations?
- **Documentation:** Are the code blocks commented to explain the transformations?
