#Case 2: Read Entire File into RAM + COUNTER

from collections import Counter 
import time
import re

# Clean item so that only English text remains

if __name__ == '__main__':

    K = 10     # Some K value
    
    print("Case 2: Read Entire File into RAM + use COUNTER")
    
    # Pick filename, uncomment the file to run
    
    filename = "/home/sanskriti/Documents/workspace/bigdata/dataset_updated/dataset_updated/data_300MB.txt"


    start1 = time.time()
    
    words = {}
    
    # Opening entire file
    with open(filename) as f:
        reader = f.readlines()

    stopwordlist = []
    with open("/home/sanskriti/Documents/workspace/bigdata/stopwords.txt") as stopwords_file:
        stopwords = stopwords_file.readlines()
        for word in stopwords:
            stopwordlist.append(word.strip('\n'))

    # Reading the contents and populating the dictionary
    for line in reader:
        for w in line.lower().split():
                if w != "'":
                    if w in words and w not in stopwordlist:
                        words[w] += 1
                    else: 
                        words[w] = 1
    
    end1 = time.time()
    
    start2 = time.time()
    
    # Sorting with counter
    count_all = Counter(words)
    
    end2 = time.time()
    
    print("Reading file + adding words and frequency to dictionary took: ", end1 - start1)    
    print("Sorting took: ", end2 - start2)
    print("Total execution time: ", end2 - start1)
    
    #Print top K words
    topKwords = count_all.most_common(K)
    print(topKwords)




