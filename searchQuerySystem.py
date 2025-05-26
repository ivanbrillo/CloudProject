import time
import os

def load_index(path):
    index = {}  # Dictionary to store the inverted index: word -> set of filenames
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            parts = line.strip().split()
            if not parts:
                continue
            
            term = parts[0] # Extract the word
            files = {entry.split(':')[0] for entry in parts[1:]}    # The list of entries like 'doc1.txt:3', 'doc2.txt:1'

            index[term] = files # Map the word to the set of filenames
    return index

def build_inverted_index(directory):
    inverted_index = {}
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        file_index = load_index(filepath)
        for term, files in file_index.items():
            inverted_index[term] = set(files)
    return inverted_index

def search_query(inverted_index, query):
    terms = query.strip().split()   # Split the query into individual terms

    result_files = inverted_index.get(terms[0], set()).copy()

    for term in terms[1:]:
        result_files &= inverted_index.get(term, set())

    return result_files


def main():
    directory_path = 'outputPython10/1GB'
    print("Loading Inverted Index ...")
    inverted_index = build_inverted_index(directory_path)
    print("Inverted Index loaded")

    print("Enter your query (e.g., cloud computing). Press Enter to exit.")

    while True:
        query = input("\nQuery > ").strip()   # Read user input and remove leading/trailing spaces
        
        if not query:  # Exit if the input is empty (user just pressed Enter)
            break   

        result = search_query(inverted_index, query)

        if result:
            print(f"Found in: {sorted(result)}")
        else:
            print("No documents found!")


if __name__ == "__main__":
    main()