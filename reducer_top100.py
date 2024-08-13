"""Reducer for Hadoop job"""

import sys

# Create an empty dictionary to store the word counts
word_counts = {}

# Read the mapper or combiner output from standard input
for line in sys.stdin:
    # Split the line by the tab character to separate the word from its count
    line = line.split("\t")
    
    # Extract the word and its count from the line
    current_word, current_count = line
    
    # Convert the count to an integer, and skip the line if it can't be converted
    try:
        current_count = int(current_count)
    except ValueError:
        continue

    # Update the count for the current_word in the word_counts dictionary
    try:
        word_counts[current_word] += current_count
    except KeyError:
        word_counts[current_word] = current_count

# Sort the words in the dictionary by their counts, and print the top 100
count = 0
for word in sorted(word_counts, key=word_counts.get, reverse=True):
    # Print the word and its count, separated by a tab character
    print(word + '\t' + str(word_counts[word]))
    
    # Count the number of words printed so far, and break the loop if we've printed 100 words
    count += 1
    if count == 100:
        break
