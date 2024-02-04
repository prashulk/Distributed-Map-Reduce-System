# Distributed-Map-Reduce-System

## Architecture –
The system is designed for distributed processing of text data stored in Google Cloud Storage. The workflow involves triggering cloud functions in response to changes in the Cloud Storage bucket. The architecture consists of multiple components, including functions for mapping, reducing, and a master function coordinating the overall workflow.

### Components -

**- Master Cloud Function (master.py):** List all files in the bucket with a specified prefix. Generate map payloads for each source file, including the number of mappers. Flatten the map payloads to cover multiple mappers for each source file. Invoke map functions in parallel using threading for each flattened map payload. Wait for all map operations to complete using a barrier. Print a message indicating the completion of all map operations. Start reduce operations by generating reduce payloads for each reducer. Invoke reduce functions in parallel using threading for each reducer. Wait for all reduce operations to complete using a barrier. This function writes 3 separate files per each reducer. Later the 3 files are combined since the end goal is that the user may enter any query word and it should display in what documents each word’s count is how much.
Triggered through: HTTP request or Cloud Storage event trigger.

**- Mapping Cloud Function (map_final.py):** The function tokenizes the input text into words using a regular expression. Divide the words into chunks for parallel processing based on the number of mappers. For each word in the chunk, create or update a word mapping dictionary. The dictionary structure: {word: {source_name: [counts]}}.
Each mapper creates word mappings for its assigned chunk. Intermediate results are saved in temporary files. Upload temporary files to Cloud Storage. Use ThreadPoolExecutor to parallelize the processing of multiple files. Wait for all tasks to complete.

**- Reduce Word Count function (reduce_final.py):** Reduce word counts by aggregating results from multiple mappers based on ASCII-based hash values. Retrieve input parameters from the HTTP request payload. Initialize a dictionary to store the reduced results. Iterate through intermediate results stored in Cloud Storage. For each word, use the ASCII-based hash to determine the assigned reducer. If the reducer index matches the current reducer, aggregate counts for the word. Upload the final reduced 3 files results to Cloud Storage.

**- Streaming Cloud Function (trigger.py):** The function is triggered by a change in the specified Cloud Storage bucket. Extract bucket name and file name from the trigger data. Check if the file name follows the expected pattern. If the pattern matches, trigger the master function to process the new file. Upon successful processing by the master function, append the results to the combined results file.

### Execution - 

Run below code in VM

```
curl -X POST https://us-central1-prashul-kumar- fall2023.cloudfunctions.net/master

```

### Output - 
[![Screenshot-2024-02-03-at-8-35-22-PM.png](https://i.postimg.cc/mgJj8XpK/Screenshot-2024-02-03-at-8-35-22-PM.png)](https://postimg.cc/FYb0KZ4x)

