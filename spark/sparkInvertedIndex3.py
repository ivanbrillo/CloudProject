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
    partitions = int(sys.argv[3]) if len(sys.argv) > 3 else 8  # Default a 8 partizioni

    # Create a Spark session with explicit parallelism
    spark = SparkSession.builder \
        .appName("invertedIndex") \
        .config("spark.default.parallelism", str(partitions * 2)) \
        .getOrCreate()
    sc = spark.sparkContext

    # Read files line by line into a DataFrame (one row per line of text)
    df = spark.read.text(input_path)

    # SEMPRE repartition dell'input per forzare parallelismo
    df = df.repartition(partitions)
    print(f"Input partitions: {df.rdd.getNumPartitions()}")

    # Add a column with the full filename each line came from
    df = df.withColumn("filename", input_file_name())

    # Converti a RDD: (filename, line)
    rdd = df.rdd.map(lambda row: (row["filename"].split("/")[-1], row["value"]))
    print(f"RDD partitions after map: {rdd.getNumPartitions()}")

    # Emit ((word, filename), 1) for each word
    words = rdd.flatMap(generate_words_from_line)
    print(f"Words partitions: {words.getNumPartitions()}")

    # Count occurrences of each (word, filename)
    counts = words.reduceByKey(lambda a, b: a + b)
    print(f"Counts partitions: {counts.getNumPartitions()}")

    # Group by word -> (word, [(filename, count), ...])
    invertedIndex = (counts
                .map(lambda wordDocs_count: (wordDocs_count[0][0], [(wordDocs_count[0][1], wordDocs_count[1])]))
                .reduceByKey(lambda a, b: a + b))
    
    print(f"InvertedIndex partitions: {invertedIndex.getNumPartitions()}")

    invertedIndexSorted = invertedIndex.sortBy(lambda x: x[0])
    print(f"Sorted partitions: {invertedIndexSorted.getNumPartitions()}")

    # Format as: word file1:count1 file2:count2 ...
    invertedIndexFormatted = invertedIndexSorted.map(
                lambda word_docList: word_docList[0] + "\t" + "\t".join(f"{fn}:{cnt}" for fn, cnt in word_docList[1]))

    # NON fare repartition finale - mantieni il parallelismo!
    # Usa coalesce solo se hai troppe partizioni piccole
    final_partitions = min(partitions, invertedIndexFormatted.getNumPartitions())
    if invertedIndexFormatted.getNumPartitions() > partitions * 2:
        invertedIndexFormatted = invertedIndexFormatted.coalesce(partitions)
    
    print(f"Final partitions before save: {invertedIndexFormatted.getNumPartitions()}")

    invertedIndexFormatted.saveAsTextFile(output_path)

    spark.stop()