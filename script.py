import json
import os
import re
import shutil
import random

def empty_folder(path):
    for entry in os.listdir(path):
        full_path = os.path.join(path, entry)
        try:
            if os.path.isfile(full_path) or os.path.islink(full_path):
                os.unlink(full_path)
            elif os.path.isdir(full_path):
                shutil.rmtree(full_path)
        except Exception as e:
            print(f"Error deleting {full_path}: {e}")

def get_folder_size(path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size

def main():
    input_dir = 'wikipedia'
    output_dir = '10documentsTXT'

    folder_limits = [
        ('512KB', 512 * 1024),
        ('1MB', 1 * 1024 * 1024),
        ('512MB', 512 * 1024 * 1024),
        ('1GB', 1 * 1024 * 1024 * 1024),
        ('5GB', 5 * 1024 * 1024 * 1024),
    ]

    json_files = [f for f in os.listdir(input_dir) if f.endswith('.json')]

    os.makedirs(output_dir, exist_ok=True)
    empty_folder(output_dir)

    for folder_name, folder_dim in folder_limits:
        folder_dir = os.path.join(output_dir, folder_name)
        os.makedirs(folder_dir, exist_ok=True)

        max_file_count = 10

        weights = [random.random() for _ in range(max_file_count)]
        total = sum(weights)
        normalized_weights = [w / total for w in weights]

        temp_limits = [int(folder_dim * w) for w in normalized_weights]

        diff = folder_dim - sum(temp_limits)
        if diff > 0:
            temp_limits[0] += diff

        file_size_limits = temp_limits

        current_file_index = 0
        current_file_size = 0
        current_file_path = os.path.join(folder_dir, f'doc{current_file_index + 1}.txt')
        current_file = open(current_file_path, 'w', encoding='utf-8')

        stop_processing = False

        for json_file in json_files:
            json_path = os.path.join(input_dir, json_file)

            with open(json_path, 'r', encoding='utf-8') as file:
                articles = json.load(file)

            for article in articles:
                body = article['text']
                body = body.replace('. ', '.\n')

                estimated_size = len(body.encode('utf-8'))
                current_file.write(body)
                current_file_size += estimated_size

                if current_file_size >= file_size_limits[current_file_index]:
                    current_file.close()
                    current_file_index += 1
                    if current_file_index >= max_file_count:
                        stop_processing = True
                        break
                    current_file_path = os.path.join(folder_dir, f'doc{current_file_index + 1}.txt')
                    current_file = open(current_file_path, 'w', encoding='utf-8')
                    current_file_size = 0

            if stop_processing:
                break

        current_file.close()
        actual_size = get_folder_size(folder_dir)
        print(f"{folder_name} completata. Spazio effettivo: {actual_size / (1024*1024):.2f}MB")

if __name__ == "__main__":
    main()