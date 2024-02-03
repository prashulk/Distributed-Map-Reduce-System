from google.cloud import storage
import json
import time

def ascii_based_hash(word, num_reducers):
    ascii_sum = sum(ord(char) for char in word)

    return ascii_sum % num_reducers

def reduce_word_count(request):
    request_json = request.get_json(silent=True)
    print(request_json)
    output_bucket = request_json.get('output_bucket')
    num_reducers = request_json.get('num_reducers')

    storage_client = storage.Client()
    bucket = storage_client.bucket(output_bucket)

    input_prefix = "intermediate/"
    reduced_results = {}

    start_time = time.time()

    for blob in bucket.list_blobs(prefix=input_prefix):
        file_name = blob.name[len(input_prefix):]

        intermediate_results = json.loads(blob.download_as_text())
        for word, sources in intermediate_results.items():
            # Use the ASCII-based hash to determine the reducer for this word
            reducer_index = ascii_based_hash(word, num_reducers)

            if reducer_index == request_json.get('reducer_index'):
                for source, counts in sources.items():
                    reduced_results.setdefault(word, {}).setdefault(source, 0)
                    reduced_results[word][source] += sum(counts)

    end_time = time.time()

    output_file_name = f"{input_prefix.rstrip('/').replace('/', '_')}_{request_json.get('reducer_index')}_reduced_final.json"
    output_blob = bucket.blob(output_file_name)
    output_blob.upload_from_string(json.dumps(reduced_results), content_type='application/json')

    elapsed_time = end_time - start_time

    return f"Reducer {request_json.get('reducer_index')} completed in {elapsed_time} seconds. Final result stored in GCS: {output_file_name}"