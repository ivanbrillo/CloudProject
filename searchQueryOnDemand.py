import os

def search_query_on_demand(directory, query):
    terms = query.split()   # Split the query into individual terms

    term_to_files = {term: set() for term in terms}

    # Iterate through all files
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split()
                if not parts:
                    continue

                term = parts[0] # Extract the word
                if term in term_to_files:
                    files = {entry.split(':')[0] for entry in parts[1:]}    # The list of entries like 'doc1.txt:3', 'doc2.txt:1'
                    term_to_files[term].update(files)   # Set union

    # Intersect file sets for all terms to find common documents
    result_files = None
    for files in term_to_files.values():
        if result_files is None:
            result_files = files
        else:
            result_files &= files   # Set intersection

    return result_files or set()


def main():
    directory_path = 'python/outputPython10/1GB'
    print("Enter your query (e.g., cloud computing). Press Enter to exit.")

    while True:
        query = input("\nQuery > ").strip().lower() # Read user input and remove leading/trailing spaces
        
        if not query:   # Exit if the input is empty (user just pressed Enter)
            break

        result = search_query_on_demand(directory_path, query)

        if result:
            print(f"Found in: {result}")
        else:
            print("No documents found!")


if __name__ == "__main__":
    main()