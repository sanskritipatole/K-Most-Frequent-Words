"""The Hadoop Reducer"""

#Importing necessary libraries
import sys
import re
# from loguru import logger
import time 

#Starting the timer
start= time.time()

#Creating a dictionary to store word counts
words = {}
 
# Reading in the mapper OR combiner output
for line in sys.stdin:
    line = line.split("\t")

    # Extracting the word and its count from the array
    curr_word, cnt = line
    # Converting count from string to integer
    try:
        cnt = int(cnt)
    except ValueError:
        # If the count is not a valid integer, skip the line
        continue

    # Splitting for word and count i.e. <word, 1> 

    # Storing the count of the current word in the dictionary
    try:
        words[curr_word] = words[curr_word]+cnt
    except:
        words[curr_word] = cnt

# Sorting and printing words and final counts
stop = 0
for word in sorted(words, key=words.get, reverse=True):
    print('{0}\t{1}'.format(word, words[word]))
    stop += 1
    if stop == 10:
        
        break

    
#Stopping the timer
end= time.time()
    # logger.info(f"The time taken in mapper is {end - start}")
print("The time taken in reducer is : ", end - start)

    
