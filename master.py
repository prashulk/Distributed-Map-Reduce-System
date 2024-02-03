import json
from google.cloud import storage
from concurrent.futures import ThreadPoolExecutor
import threading
import requests
import time

map_barrier = threading.Barrier(3) 
reduce_barrier = threading.Barrier(3)

def list_files_with_prefix(bucket_name, prefix):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    blobs = bucket.list_blobs(prefix=prefix)
    files = [blob.name for blob in blobs]

    return files

def invoke_cloud_function(payload, phase):

    function_url = "https://us-central1-prashul-kumar-fall2023.cloudfunctions.net/map_function"

    payload_json = json.dumps(payload)

    headers = {"Content-Type": "application/json"}

    start_time = time.time() 

    response = requests.post(function_url, data=payload_json, headers=headers)

    map_barrier.wait()

    end_time = time.time()  

    print(f"{payload['file_names'][0]} processed by {phase}. Elapsed time: {end_time - start_time} seconds")

    return response.text

def invoke_reducer(payload):

    function_url = "https://us-central1-prashul-kumar-fall2023.cloudfunctions.net/reduce_word_count"

    payload_json = json.dumps(payload)

    headers = {"Content-Type": "application/json"}

    start_time = time.time()

    try:
        response = requests.post(function_url, data=payload_json, headers=headers)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Reducer task failed with an exception: {e}")
    finally:
        reduce_barrier.wait()

        end_time = time.time() 

        print(f"Reducer {payload['reducer_index']} completed. Elapsed time: {end_time - start_time}")

def combine_reducer_results(bucket_name, num_reducers):
    combined_results = {}

    storage_client = storage.Client()

    for i in range(num_reducers):
        filename = f"intermediate_{i}_reduced_final.json"
        try:
            bucket = storage_client.get_bucket(bucket_name)
            blob = bucket.blob(filename)
            content = blob.download_as_text()
            data = json.loads(content)

            if isinstance(data, dict): 
                for word, sources in data.items():
                    if word in combined_results:
                        combined_results[word].update(sources)
                    else:
                        combined_results[word] = sources
            else:
                print(f"Unsupported content type in {filename}")

        except FileNotFoundError:
            print(f"File {filename} not found.")
        except Exception as e:
            print(f"Error reading {filename}: {e}")

    combined_blob = storage_client.bucket(bucket_name).blob("combined_results.json")
    combined_blob.upload_from_string(json.dumps(combined_results, indent=2))
    print("Combined results saved to 'combined_results.json' in the bucket.")

def process_barrier(request):
    bucket_name = "praskumabucket"
    filename_prefix = "book"
    output_bucket = "praskumabucket"
    num_mappers = 3
    num_reducers = 3

    source_names = list_files_with_prefix(bucket_name, filename_prefix)

    if not source_names:
        print(f"No files found with the prefix '{filename_prefix}' in the bucket '{bucket_name}'. Exiting.")
        return "No files found. Exiting."

    map_payloads = [
        {"file_names": [source_name], "output_bucket": output_bucket, "num_mappers": num_mappers}
        for source_name in source_names
    ]

    flattened_map_payloads = [
        {"file_names": [source_name], "output_bucket": output_bucket, "num_mappers": num_mappers}
        for source_name in source_names
        for _ in range(num_mappers)
    ]

    with ThreadPoolExecutor(max_workers=len(flattened_map_payloads)) as map_executor:
        map_futures = [
            map_executor.submit(invoke_cloud_function, payload, "map") for payload in flattened_map_payloads
        ]

        for map_future in map_futures:
            map_future.result()

    print("All map operations completed. Starting reduce operations...")

    reduce_payload = {"output_bucket": output_bucket, "num_reducers": num_reducers}

    with ThreadPoolExecutor(max_workers=num_reducers) as reducer_executor:
        reducer_futures = [
            reducer_executor.submit(invoke_reducer, {**reduce_payload, "reducer_index": i}) for i in range(num_reducers)
        ]

        for reducer_future in reducer_futures:
            reducer_future.result()

    print("Reducer tasks completed.")

    combine_reducer_results(bucket_name, num_reducers)

    return "Processing completed."
