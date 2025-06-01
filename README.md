# Cloud Computing Project - Inverted Index Implementation

This project implements an inverted index system using different technologies and approaches: Hadoop, Spark, and Python. The project compares the performance and results of these different implementations.

## Project Structure

### Root Directory
- `searchQuerySystem.py` and `searchQueryOnDemand.py`: Two different Python scripts implementing alternative methods for querying the inverted index system
- `script.py`: Python script for creating the dataset from the original data

### invertedIndex/
Contains Hadoop implementation files:
- `src/`: Source code directory containing both standard Hadoop and Hadoop In-Mapper Combiner Java implementations
- `target/`: Compiled Java classes and build artifacts
- `pom.xml`: Maven configuration file for the Java project
- `output_yarn.txt` and `output_log.txt`: Log files containing execution information and metrics
- `hadoop_tocsv.sh`: Shell script for running the standard Hadoop implementation and converting its output to CSV format
- `hadoopInMapper_tocsv.sh`: Shell script for running the Hadoop In-Mapper Combiner implementation and converting its output to CSV format
- `hadoopInMapperCombiner.csv`: CSV file containing results from the in-mapper combiner implementation
- `hadoop.csv`: CSV file containing results from the standard Hadoop implementation

### spark/
Contains Spark implementation files:
- `sparkInvertedIndex.py` and `sparkInvertedIndex2.py`: Two different implementations of the inverted index using Spark
- `spark_log.txt`, `output_yarn.txt`, and `executors.json`: Log files containing execution information and metrics that are used to extract performance insights
- `spark2.sh` and `spark.sh`: Shell scripts for running Spark jobs
- `spark2.csv` and `spark.csv`: CSV files containing Spark results

### python/
Contains Python implementation files:
- `invertedIndex.py`: Python script implementing the inverted index
- `resultsPython.csv`: CSV file containing Python implementation results
