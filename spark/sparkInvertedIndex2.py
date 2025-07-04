from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name
from pyspark import SparkContext
import sys
import re

# Regex to remove unwanted characters (keep letters, digits, spaces)
CLEANER = re.compile(r"[^a-zA-Z0-9\s]")

def preprocess_text(text):
    return CLEANER.sub(' ', text).lower()

def generate_words_from_line(file_line):
    index = []
    fileName, line = file_line
    words = preprocess_text(line).split()
    for word in words:
        index.append(((word, fileName), 1))
    return index

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: sparkInvertedIndex.py <input path> <output path>", file=sys.stderr)
        sys.exit(-1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    partitions = int(sys.argv[3]) if len(sys.argv) > 3 else None

    # Create a Spark session that support DataFrames
    spark = SparkSession.builder.appName("invertedIndex2").getOrCreate()
    sc = spark.sparkContext

    # Read files with the filename
    df = spark.read.text(input_path).withColumn("filename", input_file_name())

    # Optionally repartition
    if partitions and partitions > 0:
        df = df.repartition(partitions)

    print("Number of partitions:", df.rdd.getNumPartitions())

    # Convert to RDD: (filename, line)
    rdd = df.rdd.map(lambda row: (row["filename"].split("/")[-1], row["value"]))

    # Emit ((word, filename), 1) for each word
    words = rdd.flatMap(generate_words_from_line)

    # Count occurrences of each (word, filename) -> ((word, filename), N)
    counts = words.reduceByKey(lambda a, b: a + b)

    # Group by word -> (word, [(filename, count), ...])
    invertedIndex = (counts
                .map(lambda wordDocs_count: (wordDocs_count[0][0], [(wordDocs_count[0][1], wordDocs_count[1])]))    # [(word,(filename,N))]
                .reduceByKey(lambda a, b: a + b)    # list concatenation (word, [(filename, count), ...])
            )

    # Format as: word file1:count1 file2:count2 ...
    invertedIndexFormatted = invertedIndex.map(
                lambda word_docList: word_docList[0] + "\t" + "\t".join(f"{fn}:{cnt}" for fn, cnt in word_docList[1]))
    
    # Optionally repartition output
    if partitions and partitions > 0:
        invertedIndexFormatted = invertedIndexFormatted.repartition(partitions)

    invertedIndexFormatted.saveAsTextFile(output_path)

    spark.stop()