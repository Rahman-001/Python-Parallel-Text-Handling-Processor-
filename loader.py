import pandas as pd
import time
import os
import tkinter as tk
from tkinter import filedialog
from concurrent.futures import ProcessPoolExecutor, as_completed

# --- MODULE: RULE CHECKER AND SCORER (Worker) ---
def analyze_chunk(chunk_df, column_name):
    pid = os.getpid()
    try:
        chunk_df = chunk_df.copy()
        
        # 1. FIRST FILTER: Pattern finding (removing very short text)
        chunk_df = chunk_df[chunk_df[column_name].str.len() > 3]
        
        # 2. RULE-BASED SCORING: Simple Sentiment/Feeling Rules
        def get_score(text):
            text = str(text).lower()
            pos = ['good', 'great', 'excellent', 'happy', 'success', 'provisional']
            neg = ['bad', 'poor', 'fail', 'error', 'slow', 'size']
            score = sum(1 for w in pos if w in text) - sum(1 for w in neg if w in text)
            return score

        chunk_df['feeling_score'] = chunk_df[column_name].apply(get_score)
        chunk_df['processed_by_core'] = pid
        return chunk_df
    except Exception as e:
        return str(e)

# --- MODULE: SEARCH CHECKER AND FILE SAVER (Orchestrator) ---
def main():
    # 1. Load Data
    root = tk.Tk(); root.withdraw()
    print(">>> Select CSV for Batch Check...")
    file_path = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
    if not file_path: return

    try:
        df = pd.read_csv(file_path)
        text_cols = df.select_dtypes(include=['object']).columns.tolist()
        target_col = 'text_content' if 'text_content' in df.columns else \
                     df[text_cols].astype(str).apply(lambda s: s.str.len().mean()).idxmax()
    except Exception as e:
        print(f"Error: {e}"); return

    # 2. Parallel Loading & Scoring
    max_workers = os.cpu_count() - 1 or 1
    chunk_size = len(df) // max_workers if len(df) > max_workers else 1
    chunks = [df.iloc[i:i + chunk_size] for i in range(0, len(df), chunk_size)]

    print(f"--- Processing Batch: {len(df)} rows on {max_workers} Cores ---")
    results = []
    start_time = time.time()
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(analyze_chunk, chunk, target_col) for chunk in chunks]
        for f in as_completed(futures):
            res = f.result()
            if isinstance(res, pd.DataFrame): results.append(res)

    # 3. Aggregation & Search Checker
    if results:
        final_df = pd.concat(results)
        
        # Search Feature: Automatically filter for a keyword (e.g., 'Industry' or 'Total')
        # This simulates the "Search Checker" module
        keyword = "total" 
        search_results = final_df[final_df[target_col].str.contains(keyword, case=False, na=False)]
        
        # 4. Email Summary Generation (Simulation)
        avg_score = final_df['feeling_score'].mean()
        summary_report = f"""
        --- TEXT ANALYSIS SUMMARY REPORT ---
        Total Rows Processed: {len(final_df)}
        Search Term Used: '{keyword}'
        Search Matches Found: {len(search_results)}
        Average Feeling Score: {avg_score:.2f}
        Processing Time: {time.time() - start_time:.2f}s
        Status: Ready for Language Expert Review.
        """
        print(summary_report)

        # 5. File Saver
        output_name = "group_batch_report.csv"
        final_df.to_csv(output_name, index=False)
        print(f">>> Full report saved: {output_name}")
        
    else:
        print("Backend Error: No results generated.")

if __name__ == '__main__':
    main()