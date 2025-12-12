import pandas as pd
import time
import os
from concurrent.futures import ProcessPoolExecutor, as_completed

# --- PART A: The Worker Function ---
# (In the real project, the Text Analysis Developer gives you this function.
# For now, we simulate it.)
def analyze_chunk(chunk_df, chunk_id):
    """
    This function runs on a separate CPU core.
    It takes a piece of the data, processes it, and returns the result.
    """
    pid = os.getpid() # Get the ID of the process running this
    print(f"  -> Worker (Core {pid}) started Chunk {chunk_id}...")
    
    # Simulate heavy text analysis work (sleep for 1 second)
    time.sleep(1) 
    
    # Example logic: Count word length (Pandas operation)
    chunk_df['word_count'] = chunk_df['text_content'].apply(lambda x: len(str(x).split()))
    chunk_df['processed_by_core'] = pid
    
    return chunk_df

# --- PART B: The Parallel Orchestrator (YOUR ROLE) ---
def main():
    file_path = "large_sample_data.csv"
    
    # 1. SETUP: Check CPU Cores
    # We leave 1 core free for the OS/Main process so the computer doesn't freeze
    max_workers = os.cpu_count() - 1 
    if max_workers < 1: max_workers = 1
    
    print(f"--- Starting Parallel Processor on {max_workers} Cores ---")

    # 2. PANDAS: Load and Split (The "Text Breaker")
    print("Loading data...")
    # Read the CSV
    df = pd.read_csv(file_path)
    total_rows = len(df)
    
    # Calculate chunk size (Total rows / Number of CPUs)
    chunk_size = total_rows // max_workers
    
    # Split the big DataFrame into a list of smaller DataFrames
    chunks = [df.iloc[i:i + chunk_size] for i in range(0, total_rows, chunk_size)]
    print(f"Data split into {len(chunks)} chunks of approx {chunk_size} rows each.")

    # 3. CONCURRENT.FUTURES: The Execution Engine
    results = []
    start_time = time.time()

    # Create the Process Pool
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit tasks to the pool
        # This maps the 'analyze_chunk' function to our data 'chunks'
        future_to_chunk = {executor.submit(analyze_chunk, chunk, i): i for i, chunk in enumerate(chunks)}
        
        print("Tasks submitted. Waiting for results...")
        
        # 4. QUEUE LOGIC: Collecting Results
        # 'as_completed' acts like a Queue. As soon as a worker finishes, 
        # it yields the result here.
        for future in as_completed(future_to_chunk):
            try:
                data = future.result()
                results.append(data)
                print(f"  <- A Chunk finished processing. ({len(results)}/{len(chunks)} done)")
            except Exception as exc:
                print(f"Generated an exception: {exc}")

    # 5. AGGREGATION: Combine results back together
    final_df = pd.concat(results)
    end_time = time.time()
    
    print("-" * 30)
    print(f"DONE! Processed {len(final_df)} rows in {end_time - start_time:.2f} seconds.")
    print(final_df[['id', 'word_count', 'processed_by_core']].head())

if __name__ == '__main__':
    main()