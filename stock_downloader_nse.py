import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def download_nse_stock_data():
    base_url = "https://www.samco.in/bhavcopy-nse-bse-mcx"
    output_dir = "StockData"

    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created directory: {output_dir}")
    else:
        print(f"Directory already exists: {output_dir}")

    # Add headers to mimic a browser visit
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    try:
        print(f"Fetching content from: {base_url}")
        response = requests.get(base_url, headers=headers)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')

        # Find the table with specific class
        table = soup.find('table', class_='bhavcopy-table')

        if not table:
            print("Error: Could not find table with class 'bhavcopy-table'")
            # Debug: print all tables found to see if class name is different
            print(f"Tables found on page: {len(soup.find_all('table'))}")
            return

        rows = table.find_all('tr')
        cells = table.find_all('td')
        print(f"Found target table. Stats: {len(rows)} rows, {len(cells)} cells.")
        
        if len(cells) == 0:
            print("WARNING: Table found but contains no cells (td).")
            print("The content might be loaded dynamically via JavaScript or the structure matches 'th' instead of 'td'.")
            print("Trying to find links in 'th' tags as a fallback...")
            cells = table.find_all('th') # Fallback to th
        
        print("Scanning for NSE files...")
        
        # Counter for downloaded files
        download_count = 0

        # Iterate through all found cells
        for cell in cells:
            link = cell.find('a')
            
            # Check if link exists and has text ending with _NSE.csv
            if link and link.has_attr('href'):
                anchor_text = link.get_text().strip()
                
                if anchor_text.endswith('_NSE.csv'):
                    file_url = link['href']
                    
                    # Ensure absolute URL
                    full_url = urljoin(base_url, file_url)
                    
                    # Extract filename from URL
                    filename = os.path.basename(full_url)
                    
                    # Construct save path
                    save_path = os.path.join(output_dir, filename)

                    try:
                        print(f"Found match: '{anchor_text}' -> Downloading: {filename}")
                        file_response = requests.get(full_url)
                        file_response.raise_for_status()
                        
                        with open(save_path, 'wb') as f:
                            f.write(file_response.content)
                        
                        print(f"Saved to: {save_path}")
                        download_count += 1
                    except Exception as e:
                        print(f"Failed to download {full_url}: {e}")

        print(f"\nDownload complete. Total NSE files downloaded: {download_count}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    download_nse_stock_data()
