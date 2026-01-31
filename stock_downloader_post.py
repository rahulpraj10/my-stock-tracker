import requests
import os
import datetime
from datetime import timedelta
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import glob
import pandas as pd

def process_and_store_data():
    """Reads downloaded CSVs and merges them into a cumulative pickle file."""
    output_dir = "StockData"
    master_file = os.path.join(output_dir, "stock_master.pkl")
    
    print("\n--- Starting Data Processing ---")

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

def download_post_stock_data():
    url = 'https://www.samco.in/bse_nse_mcx/getBhavcopy'
    output_dir = "StockData"

    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created directory: {output_dir}")

    # Calculate dates
    today = datetime.date.today()
    
    # Python's weekday: Monday=0, Sunday=6
    # We want start_date to be Sunday of the current week
    # If today is Sunday (6), start_date is today.
    # If today is Monday (0), start_date is yesterday/Sunday.
    
    idx = (today.weekday() + 1) % 7
    start_date = today - timedelta(days=idx)
    
    # End date is Saturday of the current week
    end_date = start_date + timedelta(days=6)

    formatted_start = start_date.strftime("%Y-%m-%d")
    formatted_end = end_date.strftime("%Y-%m-%d")

    print(f"Calculated Dates -> Start (Sun): {formatted_start}, End (Sat): {formatted_end}")

    payload = {
        'start_date': formatted_start,
        'end_date': formatted_end,
        'bhavcopy_data[]': 'NSE',
        'show_or_down': '1'
    }
    
    # Add headers to mimic a browser visit
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    try:
        print(f"Sending POST request to {url}...")
        response = requests.post(url, data=payload, headers=headers)
        response.raise_for_status()

        # Parse the HTML response
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find all <a> tags with the specific class
        links = soup.find_all('a', class_='bhavcopy-table-body-link')
        
        if not links:
            print("No links found with class 'bhavcopy-table-body-link' in the response.")
            # Debug: print first 500 chars of response to see what we got
            print("Response preview:", response.text[:500])
            return

        print(f"Found {len(links)} files to download.")
        
        download_count = 0
        
        for link in links:
            if not link.has_attr('href'):
                continue
                
            file_url = link['href']
            anchor_text = link.get_text().strip()
            
            # Use anchor text as filename, ensuring .csv extension
            filename = anchor_text
            if not filename.lower().endswith('.csv'):
                filename += '.csv'
                
            # Clean filename of illegal characters just in case
            filename = "".join([c for c in filename if c.isalpha() or c.isdigit() or c in (' ', '.', '_', '-')]).strip()
            
            # Ensure absolute URL
            full_url = urljoin("https://www.samco.in", file_url)
            
            save_path = os.path.join(output_dir, filename)
            
            try:
                print(f"Downloading: {filename} from {full_url}")
                file_response = requests.get(full_url, headers=headers)
                file_response.raise_for_status()
                
                with open(save_path, 'wb') as f:
                    f.write(file_response.content)
                
                print(f"Saved to: {save_path}")
                download_count += 1
            except Exception as e:
                print(f"Failed to download {filename}: {e}")

        print(f"\nDownload complete. Total files downloaded: {download_count}")
        
        # Trigger processing after download
        process_and_store_data()

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    download_post_stock_data()
