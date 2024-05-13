import concurrent.futures
from datetime import datetime
import geopandas as gpd
import os

def process_chunk(start_row, end_row, chunk_num):
    print(f"Chunk {chunk_num} started at {datetime.now()}")
    file_path = 'a00000010.gdbtable'
    with open(file_path, 'rb') as f:
        f.seek(start_row)
        chunk = gpd.read_file(f, engine="pyogrio", rows=slice(end_row - start_row))
    chunk.to_parquet(f'output/parquet_chunk_{chunk_num}.parquet')
    print(f"Chunk {chunk_num} completed at {datetime.now()}")

# Number of threads to use
num_threads = 4  # You can adjust this based on your system resources and requirements

# Define chunk size
chunk_size = 500000

# Create a ThreadPoolExecutor
with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
    # Submit tasks for each chunk of data
    futures = []
    chunk_num = 1
    while True:
        start_row = (chunk_num - 1) * chunk_size
        end_row = chunk_num * chunk_size
        futures.append(executor.submit(process_chunk, start_row, end_row, chunk_num))
        
        # Check if the last chunk has been processed
        if len(futures) == num_threads:
            # Wait for all threads to finish before proceeding to the next iteration
            for future in concurrent.futures.as_completed(futures):
                future.result()
            futures = []
        
        # Check if there's no more data left to read
        if end_row >= os.path.getsize(file_path):
            break
        
        chunk_num += 1

    # Wait for any remaining threads to finish
    for future in concurrent.futures.as_completed(futures):
        future.result()

print("All chunks processed successfully.")
