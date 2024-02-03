import re
import json
import os
import tempfile
from google.cloud import storage
from concurrent.futures import ThreadPoolExecutor
import time

def map_word_count_internal(book_text: str, source_name: str, mapper_name: str, mapper_index: int, total_mappers: int):
    words = re.findall(r'\b\w+\b', book_text.lower())

    total_words = len(words)
    words_per_mapper = total_words // total_mappers

    # Calculate the start and end indices for this mapper
    start_index = mapper_index * words_per_mapper
    end_index = (mapper_index + 1) * words_per_mapper if mapper_index < total_mappers - 1 else total_words

    words_chunk = words[start_index:end_index]

    word_mapping = {}
    for word in words_chunk:
        if word in word_mapping:
            if source_name in word_mapping[word]:
                word_mapping[word][source_name].append(1)
            else:
                word_mapping[word][source_name] = [1]
        else:
            word_mapping[word] = {source_name: [1]}

    return word_mapping, mapper_name

def process_file(args):
    file_name, output_bucket, mappers, total_mappers = args
    start_time = time.time()

    storage_client = storage.Client()
    bucket = storage_client.bucket(output_bucket)
    input_blob = bucket.blob(file_name)
    content = input_blob.download_as_text()

    temp_filenames = [
        os.path.join(tempfile.gettempdir(), f"{os.path.splitext(file_name)[0]}_{mapper}.json")
        for mapper in mappers
    ]

    word_mappings = []
    for mapper_name, temp_filename in zip(mappers, temp_filenames):
        word_mapping, mapper_name = map_word_count_internal(content, file_name, mapper_name, mappers.index(mapper_name), total_mappers)
        with open(temp_filename, 'w') as temp_file:
            json.dump(word_mapping, temp_file)
        word_mappings.append((mapper_name, temp_filename))

    intermediate_file_names = [
        f"intermediate/{os.path.splitext(file_name)[0]}_{mapper_name}.json"
        for mapper_name in mappers
    ]

    intermediate_blobs = [
        bucket.blob(intermediate_file_name)
        for intermediate_file_name in intermediate_file_names
    ]

    for temp_filename, intermediate_blob in zip(temp_filenames, intermediate_blobs):
        intermediate_blob.upload_from_filename(temp_filename)

    end_time = time.time() 
    elapsed_time = end_time - start_time

    return [
        f"{file_name} processed by {mapper_name}. Elapsed time: {elapsed_time} seconds"
        for mapper_name in mappers
    ]


def map_word_count(request):
    request_json = request.get_json(silent=True)
    file_names = request_json.get('file_names', [])
    output_bucket = request_json.get('output_bucket')
    num_mappers = request_json.get('num_mappers')

    results = []

    for file_name in file_names:
        args_list = [
            (file_name, output_bucket, [f"mapper{i}" for i in range(num_mappers)], num_mappers)
        ]

        if not args_list:
            results.append(f"No files to process for {file_name}")
        else:
            with ThreadPoolExecutor(max_workers=num_mappers) as executor:
                futures = [executor.submit(process_file, args) for args in args_list]

                # Wait for all tasks to complete
                file_results = [future.result() for future in futures]
                results.extend(file_results)

    return results
