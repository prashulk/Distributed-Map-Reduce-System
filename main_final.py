import json
import os
from google.cloud import storage
from flask import Flask, request, jsonify

app = Flask(__name__)
storage_client = storage.Client()

def search_documents(query, data):
    return data.get(query, {})

def get_documents(request):
    query = request.args.get('query', '')
    if not query:
        return jsonify({'error': 'Please provide a search query using the "query" parameter.'}), 400

    bucket_name = 'praskumabucket'
    file_name = 'combined_results.json'

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    try:
        content = blob.download_as_text()
        data = json.loads(content)
        result = search_documents(query, data)
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': f"Error reading {file_name}: {e}"}), 500

@app.route('/')
def search(request):
    if request.method == 'GET':
        return get_documents(request)
    else:
        return jsonify({'error': 'Invalid method. Use GET request with "query" parameter.'}), 405
