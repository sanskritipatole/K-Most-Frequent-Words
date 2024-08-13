import re
import time

# Start measuring the execution time
start_time = time.time()

# Initialize an empty list for stop words
stop_words_list = []

# Read stop words from the file and populate the stop_words_list
with open("/Users/nihu/spark_wordcount/stop_words.txt") as stopwords_file:
    stopwords = stopwords_file.readlines()
    for stopword in stopwords:
        stop_words_list.append(stopword.strip('\n'))

# Read the book data from a text file
book = sc.textFile("file:///Users/sanskriti/spark_wordcount/data_16GB.txt")

# Function to preprocess a single word
def preprocess_word(word: str):
    return re.sub("[^A-Za-z0-9]+", "", word.lower())

# Function to preprocess a list of words
def preprocess_words(words: str):
    # Preprocess each word in the list
    preprocessed = [preprocess_word(word) for word in words.split()]
    # Filter out the stop words and words with length <= 6 from the preprocessed list
    return [word for word in preprocessed if len(word) > 6 and word not in stop_words_list]

# Preprocess the words in the book
words = book.flatMap(preprocess_words)

# Map each word to a tuple of (word, 1) for counting
word_counts = words.map(lambda x: (x, 1))

# Reduce by key to get the count of each word
word_counts = word_counts.reduceByKey(lambda x, y: x + y)

# Map the word counts to (count, word) tuples for sorting
word_counts_sorted = word_counts.map(lambda x: (x[1], x[0]))

# Sort the word counts in descending order
word_counts_sorted = word_counts_sorted.sortByKey(False)

# Collect the top 10 word counts
top_word_counts = word_counts_sorted.collect()[:10]

# Stop measuring the execution time
end_time = time.time()

# Print the top 10 word counts
for count, word in top_word_counts:
    print(word, count)

# Print the execution time
print("The time taken in mapper is:", end_time - start_time)
