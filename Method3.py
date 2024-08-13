import os
import re
import time
import collections
import concurrent.futures
import pandas as pd
import matplotlib.pyplot as plt

# Define constants
CHUNK_SIZE = int(512 * 1024 * 1024)     # 256 MB
STOPWORDS_FILE = "stopwords.txt"


def save_topKwords(filename, K, topKwords):
    name = filename.split(".")
    fname = f"{name[0]}_TOP_{K}_WORDS.txt"
    with open(fname, 'w') as f:
        f.write("Frequency \t Word \n")
        for word, freq in topKwords:
            f.write(f"{freq}\t\t{word}\n")

def make_chunks(filename, chunk_size):
    if not os.path.exists(f"Parts_of_{filename}"):
        os.makedirs(f"Parts_of_{filename}")
    with open(filename, 'rb') as reader:
        p = 0
        while True:
            lines = reader.read(chunk_size)
            if not lines:
                break
            p += 1
            fname = f"Parts_of_{filename}/part_{p}"
            with open(fname, 'wb') as file_object:
                file_object.write(lines)
    return p

def read_chunks(filename, partnumber, stopwordlist):
    words = {}
    fname = f"Parts_of_{filename}/part_{partnumber}"
    with open(fname) as reader:
        for line in reader:
            for w in line.lower().split():
                if w != "'":
                    if w in words and w not in stopwordlist:
                        words[w] += 1
                    else:
                        words[w] = 1
    return words

if __name__ == '__main__':
    
    filename = "dataset_updated/data_16GB.txt"

    stopwordlist = []
    with open(STOPWORDS_FILE) as stopwords_file:
        stopwords = stopwords_file.readlines()
        stopwordlist = [word.strip('\n') for word in stopwords]

    start1 = time.time()
    totalfiles = make_chunks(filename, CHUNK_SIZE)
    filenames = [f"part_{i}" for i in range(1, totalfiles+1)]
    end1 = time.time()
    print(f"Dividing file into chunks and storing individual files: {end1 - start1}")

    start2 = time.time()
    count_all = collections.Counter()
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for result in executor.map(read_chunks, [filename]*totalfiles, range(1, totalfiles+1), [stopwordlist]*totalfiles):
            count_all.update(result)
    end2 = time.time()
    print(f"Parallel Execution took: {end2 - start2}")
    print(f"Total execution time: {end2 - start1}")

    K = int(input("How many most common words to print: "))
    topKwords = count_all.most_common(K)
    print(topKwords)
    save_topKwords(filename, K, topKwords)

    # Draw a bar chart
    topKwords = count_all.most_common(K)
    df = pd.DataFrame(topKwords, columns = ['Word', 'Count'])
    df.plot.bar(x='Word',y='Count')
    plt.title('K Most Popular Words')
    plt.xlabel('Word')
    plt.ylabel('Count')
    plt.show()

