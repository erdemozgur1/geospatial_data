import concurrent.futures
from datetime import datetime
import geopandas as gpd
import os

def func():
    file_path = 'a00000010.gdbtable'
    chunk_size = 500000 # Adjust the chunk size as needed
    start_row = 0

    # Create the output directory if it does not exist
    output_dir = 'output'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Read the first chunk
    chunk = gpd.read_file(file_path, engine="pyogrio",rows=slice(start_row, start_row + chunk_size))

    # Initialize chunk counter
    chunk_counter = 0

    # Continue reading chunks until there are no more rows
    while not chunk.empty:
        # Save the chunk to parquet
        chunk.to_parquet(f'output/parquet_chunk_{chunk_counter}.parquet')
        
        # Increment chunk counter
        chunk_counter += 1
        
        # Read the next chunk
        start_row += chunk_size
        chunk = gpd.read_file(file_path, engine="pyogrio",rows=slice(start_row, start_row + chunk_size))


# Number of threads to use
num_threads = 2

# Create a ThreadPoolExecutor
with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
    # Submit tasks for each chunk of data
    result=executor.submit(func)

     # Monitor progress and completion
    for future in concurrent.futures.as_completed(result):
        # Future.result() will block until the result is available
        result = future.result()
        # Do something with the result if needed
        print("Task completed:", result)