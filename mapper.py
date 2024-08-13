"""The Hadoop Mapper function"""

import sys
import re
# from loguru import logger
import time 

# create an empty dictionary to store the word count
words = {}

def mapper():

    # stopwords = set(["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now", "The", "But"]) 
    
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

    # Reading and processing words from text file 
    for l in sys.stdin:
        # convert the input line to lowercase and split it into words
        w = l.lower().split()
        for word in w:
            # check if the word is not a stop word
            if word not in stop_words_list:
                # print the word and its count to stdout
                print('%s\t%s' % (word, 1))

    # end the timestamp to note the time taken by mapper function
    end= time.time()
    # Log the time taken by mapper function using a logger
    # logger.info(f"The time taken in mapper is {end - start}")
    # Or simply print the time taken by mapper function
    print("The time taken in mapper is : ", end - start)

if __name__ == "__main__":
    mapper()