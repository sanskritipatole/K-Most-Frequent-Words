"""The Hadoop Mapper function for word length greater than 6 """

import sys
import re
# from loguru import logger  # import logging library for logging messages
import time 

def mapper6():

    #start the timestamp to note the time used by mapper function
    start= time.time()
    # create a list to store the stopwords imported from a file
    stop_words_list = [] 
    # access the stopwords text file  (which contains all stopwords)
    with open("/home/sanskriti/Documents/workspace/bigdata/stopwords.txt") as stopwords_file:
        stopwords = stopwords_file.readlines()
        # strip newline characters from each line and add the words to the list
        for word in stopwords:
            stop_words_list.append(word.strip('\n'))

    # Reading and processing words from text file 
    for l in sys.stdin:
        w = l.split()
        for word in w:
            # If the word length is greater than 6 and it is not a stopword
            if len(word) > 6 and word not in stop_words_list:
                # Printing <word, 1> for combiner OR reducer to use
                print('%s\t%s' % (word, 1))

    # Get the end timestamp to calculate the time taken by mapper
    end= time.time()
    # Log the time taken by mapper function using a logger
    # logger.info(f"The time taken in mapper is {end - start}")
    # Or simply print the time taken by mapper function
    print("The time taken in mapper is : ", end - start)

if __name__ == "__main__":
    mapper6()