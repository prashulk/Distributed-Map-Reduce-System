import json
from google.cloud import storage
import requests

def process_new_file(request, context):
    """Triggered by a change to a Cloud Storage bucket."""

    if isinstance(request, dict):
        data = request
    else:
        data = json.loads(request.get_data(as_text=True))

    bucket_name = data['bucket']
    file_name = data['name']

    if file_name.startswith('book') and file_name.endswith('.txt'):
        master_function_url = "https://us-central1-prashul-kumar-fall2023.cloudfunctions.net/master"

        payload = {
            "file_names": [file_name],
            "output_bucket": bucket_name,
            "num_mappers": 3,
            "num_reducers": 3
        }

        headers = {"Content-Type": "application/json"}

        response = requests.post(master_function_url, json=payload, headers=headers)

        if response.status_code == 200:
            print(f"Master function triggered successfully for file: {file_name}")
            append_to_combined(bucket_name)
        else:
            print(f"Failed to trigger master function for file: {file_name}")

def append_to_combined(bucket_name):
    storage_client = storage.Client()

    combined_results = {}

    for i in range(3): 
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
