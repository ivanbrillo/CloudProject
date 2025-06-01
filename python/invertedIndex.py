import re
import os
import time
import psutil
import sys
from collections import defaultdict
from datetime import datetime, timedelta

# Regex to match any character that is not a letter, digit, space, or apostrophe
CLEANER = re.compile(r"[^a-zA-Z0-9\s]")

input_folders = ["512KB", "1MB", "512MB", "1GB", "2GB"]

def preprocess_text(text):
    # Replace unwanted characters with space and convert to lowercase
    return CLEANER.sub(' ', text).lower()

def build_inverted_index(directory_path):    
    inverted_index = defaultdict(lambda: defaultdict(int))  # word:{fileName:count}

    # Iterate through all files in the given directory
    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)

        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:       # Process each line individually
                line = preprocess_text(line)
                for term in line.split():
                    inverted_index[term][filename] += 1
    return inverted_index


def formatter(inverted_index, output_folder, output_dir, num_parts=3):
    formatted_output = []

    for term, file_counts in inverted_index.items():
        line = f"{term}"
        for filename, count in file_counts.items():
            line += f"    {filename}:{count}"
        formatted_output.append(line)

    total_lines = len(formatted_output)
    lines_per_file = (total_lines + num_parts - 1) // num_parts     # Rounding up to ensure all lines are included

    os.makedirs(f"{output_dir}/{output_folder}", exist_ok=True)

    # Write the formatted output to multiple files
    for part in range(num_parts):
        start_index = part * lines_per_file
        end_index = min(start_index + lines_per_file, total_lines)

        part_filename = f"{output_dir}/{output_folder}/part{part + 1}.txt"

        # Open the file and write the selected portion of the output
        with open(part_filename, 'w', encoding='utf-8') as f:
            f.write("\n".join(formatted_output[start_index:end_index]))

        # Clear written lines from memory to save space
        for i in range(start_index, end_index):
            formatted_output[i] = None


def count_total_occurrences(inverted_index):
    total = 0
    for postings in inverted_index.values():
        total += sum(postings.values())
    return total


def main():
    # --- USAGE INSTRUCTION ---
    # Run this script from the terminal like this:
    # python3 invertedIndex.py <NUM_FILE> <NUM_RUNS>
    # Example: python3 invertedIndex.py 20 3
    # This will process 20 files per folder and repeat the whole execution 3 times

    if len(sys.argv) != 3:
        print("Usage: python invertedIndex.py <NUM_FILE> <NUM_RUNS>")
        sys.exit(1)

    NUM_FILE = int(sys.argv[1])
    num_runs = int(sys.argv[2])

    input_dir = f"../{NUM_FILE}documentsTXT"
    output_dir = f"outputPython{NUM_FILE}"

    os.makedirs(output_dir, exist_ok=True)

    for _ in range(num_runs):
        for folder_name in input_folders:   # Different folders dimention
            start_time = time.time()

            folder_directory = os.path.join(input_dir, folder_name)

            # Build the inverted index from the input file
            inverted_index = build_inverted_index(folder_directory)

            # Format and save the inverted index to the output file
            formatter(inverted_index, folder_name, output_dir, 2)

            end_time = time.time()

            # ---------- METRICS ----------
            execution_time_ms = (end_time - start_time) * 1000      # Calculate the execution time in milliseconds
            num_terms = len(inverted_index)                         # Count the number of unique terms in the inverted index
            tot_terms = count_total_occurrences(inverted_index)     # Count the total number of term occurrences across all documents
            terms_per_sec = tot_terms / (execution_time_ms / 1000)  # Calculate the processing speed (terms per second)

            ts = datetime.now() + timedelta(hours=2)
            ts_str   = ts.strftime("%Y-%m-%d %H:%M:%S")

            # -------- PRINT METRICS --------
            print(f"python,{folder_name},{NUM_FILE}doc,{execution_time_ms:.4f},{ts_str}")

            # Append the metrics to a result file
            with open(f"resultsPython.csv", 'a', encoding='utf-8') as f:
                f.write(
                    f"python,{folder_name},{NUM_FILE}doc,{execution_time_ms:.4f},{ts_str}\n"
                )


if __name__ == "__main__":
    main()