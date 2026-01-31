import os
import glob
import pandas as pd

def process_and_store_data():
    output_dir = "StockData"
    master_file = "stock_master.pkl"
    
    if not os.path.exists(output_dir):
        print(f"Directory {output_dir} does not exist. Nothing to process.")
        return

    # 1. Load existing master data if available
    if os.path.exists(master_file):
        try:
            master_df = pd.read_pickle(master_file)
            print(f"Loaded existing master file with {len(master_df)} rows.")
        except Exception as e:
            print(f"Error loading existing master file: {e}")
            master_df = pd.DataFrame()
    else:
        print("No existing master file found. Creating new.")
        master_df = pd.DataFrame()

    # 2. Read new CSV files
    csv_files = glob.glob(os.path.join(output_dir, "*.csv"))
    
    if not csv_files:
        print("No CSV files found to process.")
        return

    print(f"Found {len(csv_files)} CSV files. Processing...")
    
    new_data_frames = []
    
    for file in csv_files:
        try:
            # Read CSV
            df = pd.read_csv(file)
            # Add a column for source file if useful for debugging/tracking
            df['source_file'] = os.path.basename(file)
            new_data_frames.append(df)
        except Exception as e:
            print(f"Failed to read {file}: {e}")

    if not new_data_frames:
        print("No valid data extracted from CSVs.")
        return

    # 3. Concatenate new data
    new_combined_df = pd.concat(new_data_frames, ignore_index=True)
    print(f"Read {len(new_combined_df)} rows from CSV files.")

    # 4. Merge with master and deduplicate
    if not master_df.empty:
        # Concatenate old and new
        final_df = pd.concat([master_df, new_combined_df], ignore_index=True)
    else:
        final_df = new_combined_df

    # Deduplicate
    # We drop duplicates based on all columns except 'source_file' to be safe, 
    # or just all columns if strict content match is desired.
    # Assuming standard bhavcopy structure, duplicates are identical rows.
    
    initial_count = len(final_df)
    final_df = final_df.drop_duplicates()
    final_count = len(final_df)
    
    print(f"Deduplication removed {initial_count - final_count} rows.")

    # 5. Save back to pickle
    try:
        final_df.to_pickle(master_file)
        print(f"Successfully saved cumulative data to {master_file}. Total rows: {final_count}")
    except Exception as e:
        print(f"Error saving to pickle file: {e}")

if __name__ == "__main__":
    process_and_store_data()
