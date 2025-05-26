from pyspark import SparkConf, SparkContext
import sys
import re

# Regex to remove unwanted characters (keep letters, digits, spaces)
CLEANER = re.compile(r"[^a-zA-Z0-9\s]")

def preprocess_text(text):
    return CLEANER.sub(' ', text).lower()

def generate_words(directory_text):
    fileName = directory_text[0].split("/")[-1]
    index = []
    words = preprocess_text(directory_text[1]).split()
    for word in words:
        index.append(((word, fileName), 1))
    return index

if __name__ == "__main__":

    if len(sys.argv) < 3:
        print("Usage: invertedIndex.py <input path> <output path>", file=sys.stderr)
        sys.exit(-1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    conf = SparkConf().setAppName("invertedIndex")
    sc = SparkContext(conf=conf)

    # Read all files in the directory as (filename, content)
    files = sc.wholeTextFiles(input_path + "/*", minPartitions=3)

    # Emit ((word, filename), 1) for each word
    words = files.flatMap(generate_words)
    
    # Count occurrences of each (word, filename)
    counts = words.reduceByKey(lambda x, y: x + y)
    
    # Group by word -> (word, [(filename, count), ...])
    invertedIndex = (counts
                .map(lambda wordDocs_count: (wordDocs_count[0][0], [(wordDocs_count[0][1], wordDocs_count[1])]))
                .reduceByKey(lambda x, y: x + y))
        
    # Format as: word file1:count1 file2:count2 ...
    invertedIndexFormatted = invertedIndex.map(
                lambda word_docList: word_docList[0] + "\t" + "\t".join(f"{fn}:{cnt}" for fn, cnt in word_docList[1]))

    invertedIndexFormatted.saveAsTextFile(output_path)

    sc.stop()