import pandas as pd
import time
import os
import sqlite3
import tkinter as tk
from tkinter import filedialog
from concurrent.futures import ProcessPoolExecutor, as_completed

# --- TEAM CONNECTION: IMPORT PARTNER'S WORK ---
try:
    from logic import CoreLogic
    logic_tool = CoreLogic()
except ImportError:
    print("CRITICAL ERROR: logic.py not found in the same folder!")
    logic_tool = None

# --- MODULE: PARALLEL WORKER ---
def analyze_chunk(chunk_df, column_name):
    pid = os.getpid()
    try:
        chunk_df = chunk_df.copy()
        
        # We ensure the column is treated as a string before passing to Core Logic
        if logic_tool:
            chunk_df['feeling_score'] = chunk_df[column_name].astype(str).apply(logic_tool.advanced_scorer)
        else:
            chunk_df['feeling_score'] = 0
            
        chunk_df['processed_by_core'] = pid
        return chunk_df
    except Exception as e:
        return str(e)

# --- MODULE: UNIVERSAL ORCHESTRATOR ---
def main():
    # 1. UI: File Selection
    root = tk.Tk(); root.withdraw()
    print(">>> Select any CSV file for processing...")
    file_path = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
    if not file_path: return

    # 2. AGGRESSIVE LOADING & AUTO-DETECTION
    try:
        # We try common delimiters in case it's not a standard comma
        df = pd.read_csv(file_path, sep=None, engine='python')
        
        if df.empty:
            print("Error: The file is empty."); return

        # AGGRESSIVE DETECTION: 
        # Instead of filtering by type, we look at all columns.
        # We pick the column that has the longest average content.
        potential_cols = df.columns.tolist()
        target_col = df[potential_cols].astype(str).apply(lambda s: s.str.len().mean()).idxmax()
        
        print(f">>> [System] Auto-Detected Analysis Column: '{target_col}'")
        
    except Exception as e:
        print(f"Loading Error: {e}"); return

    # 3. PARALLEL ENGINE (7+ Cores)
    max_workers = os.cpu_count() - 1 or 1
    chunk_size = max(1, len(df) // max_workers)
    chunks = [df.iloc[i:i + chunk_size] for i in range(0, len(df), chunk_size)]

    

    print(f"--- Running Parallel Engine on {max_workers} Cores ---")
    results = []
    start_time = time.time()
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(analyze_chunk, chunk, target_col) for chunk in chunks]
        for f in as_completed(futures):
            res = f.result()
            if isinstance(res, pd.DataFrame): results.append(res)

    # 4. DATA INTEGRITY & STORAGE
    if results:
        final_df = pd.concat(results)
        
        # Sync to Database
        db_ready_df = pd.DataFrame()
        db_ready_df['content'] = final_df[target_col].astype(str)
        db_ready_df['score'] = final_df['feeling_score']
        db_ready_df['category'] = "Auto-Processed"
        
        if logic_tool:
            print(">>> Syncing results to Database...")
            conn = sqlite3.connect(logic_tool.db_name)
            db_ready_df.to_sql('text_data', conn, if_exists='append', index=False)
            conn.close()
        
        # 5. FINAL REPORT
        print(f"\n--- BATCH REPORT COMPLETE ---")
        print(f"Total Processed: {len(final_df)} rows")
        print(f"Processing Time: {time.time() - start_time:.2f}s")
        
        final_df.to_csv("universal_batch_report.csv", index=False)
        print("CSV Report: 'universal_batch_report.csv' created.")
    else:
        print("Engine Error: No results generated.")

if __name__ == '__main__':
    main()