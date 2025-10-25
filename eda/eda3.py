import pandas as pd
import os

# --- Configuration ---
years = [2021, 2022, 2023, 2024, 2025, 'preprint']
DATA_DIR = '../data'
# The output file name and delimiter are retained for clean viewing
OUTPUT_FILE = os.path.join(DATA_DIR, 'topic_yearly_counts.psv') 

all_counts = []
name_column = 'name' # The common column used for merging

for year in years:
    # Construct the full file path for reading
    file_path = os.path.join(DATA_DIR, f"{year}.csv")
    
    try:
        # Read the file. It is now a standard CSV (Topic Name, Count)
        # We will keep the sep=None and engine='python' for robustness against weird formatting.
        df = pd.read_csv(file_path, sep=None, engine='python')

        # Check for required columns
        if name_column not in df.columns or 'count' not in df.columns:
             print(f"⚠️ Error: File {file_path} must contain '{name_column}' and 'count' columns. Skipping.")
             continue
        
        # Rename the 'count' column to include the year, and drop the Topic Name column (to be used later).
        counts_df = df.rename(columns={'count': f'{year}_Count'})
        
        # Select only the Topic Name and the new yearly count column
        counts_df = counts_df[[name_column, f'{year}_Count']]

        all_counts.append(counts_df)
        print(f" Processed summary for {year}.")

    except FileNotFoundError:
        print(f" Error: File {file_path} not found. Skipping.")
    except Exception as e:
        print(f" An unexpected error occurred while processing {file_path}: {e}")


if all_counts:
    final_df = all_counts[0]
    
    for i in range(1, len(all_counts)):
        final_df = final_df.merge(all_counts[i], on=name_column, how='outer')

    final_df = final_df.fillna(0)
    
    count_columns = [f'{year}_Count' for year in years]
    final_df[count_columns] = final_df[count_columns].astype(int)

    final_df = final_df.rename(columns={name_column: 'Primary Topic Id'})
    final_df = final_df.sort_values(by='Primary Topic Id').reset_index(drop=True)
    
    print("\n Successfully merged all yearly summaries.")
    final_df.to_csv(OUTPUT_FILE, index=False, sep=',') 
    
else:
   pass