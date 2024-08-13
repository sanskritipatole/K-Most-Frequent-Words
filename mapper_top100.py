"""The Hadoop Mapper for Top 100 words"""

import sys
import re
# from loguru import logger
import time 

words = {}

# start the timestamp to note the time used by mapper function
start= time.time()

# create a list to store the stopwords imported from
stop_words_list = []

# access the stopwords text file (which contains all stopwords)
with open("/home/sanskriti/Documents/workspace/bigdata/stopwords.txt") as stopwords_file:
    # read all the stopwords in the file
    stopwords = stopwords_file.readlines()
    # strip the newline character from each word and add it to the stop_words_list
    for word in stopwords:
        stop_words_list.append(word.strip('\n'))

# Reading and processing words from output directory
for l in sys.stdin:
    w = l.lower().split()
    for word in w:
    # check if the word is not a stop word
        if word not in stop_words_list:
            curr_word, count = l
            print(curr_word + '\t' + str(count))
