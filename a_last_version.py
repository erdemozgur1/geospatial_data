import concurrent.futures
from datetime import datetime
import geopandas as gpd
import os

def process_chunk(file_path, start_row, chunk_size, chunk_counter):
    print(f"chunk {chunk_counter} started")
    chunk = gpd.read_file(file_path, engine="pyogrio", rows=slice(start_row, start_row + chunk_size))
    output_file = f'output/parquet_chunk_th_{chunk_counter}.parquet'
    chunk.to_parquet(output_file)
    print(f"chunk_{chunk_counter} completed")

def main():
    file_path = 'a00000010.gdbtable'
    chunk_size = 500000  # Adjust the chunk size as needed
    start_row = 0
    output_dir = 'output'

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    chunk_counter = 0
    futures = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        while True:
            chunk_future = executor.submit(process_chunk, file_path, start_row, chunk_size, chunk_counter)
            futures.append(chunk_future)
            chunk_counter += 1
            start_row += chunk_size
            chunk = gpd.read_file(file_path, engine="pyogrio", rows=slice(start_row, start_row + chunk_size))
            if chunk.empty:
                break

    # Monitor progress and completion
    for future in concurrent.futures.as_completed(futures):
        result = future.result()
        print("Task completed:", result)

if __name__ == "__main__":
    num_threads = 2
    main()
