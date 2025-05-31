import os

def search_query_on_demand(directory, query):
    terms = query.strip().split()
    if not terms:
        return set()

    term_to_files = {term: set() for term in terms}

    # Iterate through all files
    for filename in os.listdir(directory):
        if not filename.endswith('.txt'):
            continue

        filepath = os.path.join(directory, filename)
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split()
                if not parts:
                    continue

                term = parts[0]
                if term in term_to_files:
                    files = {entry.split(':')[0] for entry in parts[1:]}
                    term_to_files[term].update(files)

    # Intersect file sets for all terms to find common documents
    result_files = None
    for files in term_to_files.values():
        if result_files is None:
            result_files = files
        else:
            result_files &= files

    return result_files or set()


def main():
    directory_path = 'outputPython/1GB'
    print("Enter your query (e.g., cloud computing). Press Enter to exit.")

    while True:
        query = input("\nQuery > ").strip()
        if not query:
            break

        result = search_query_on_demand(directory_path, query)

        if result:
            print(f"Found in: {result}")
        else:
            print("No documents found!")


if __name__ == "__main__":
    main()
