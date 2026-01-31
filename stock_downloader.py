import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def download_stock_data():
    base_url = "https://www.samco.in/bhavcopy-nse-bse-mcx"
    output_dir = "StockData"

    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created directory: {output_dir}")
    else:
        print(f"Directory already exists: {output_dir}")

    try:
        print(f"Fetching content from: {base_url}")
        response = requests.get(base_url)
        response.raise_for_status() # Raise an error for bad status codes

        soup = BeautifulSoup(response.content, 'html.parser')

        # Find the table with specific class as requested
        table = soup.find('table', class_='bhavcopy-table')

        if not table:
            print("Error: Could not find table with class 'bhavcopy-table'")
            return

        print("Found target table. Scanning for files...")
        
        # Counter for downloaded files
        download_count = 0

        # Find web links in td elements
        # The user mentioned "hyperlink to the file location is available in the td elemnts"
        # So we look for <a> tags inside <td> tags
        for row in table.find_all('tr'):
            for cell in row.find_all('td'):
                link = cell.find('a')
                if link and link.has_attr('href'):
                    file_url = link['href']
                    
                    # Ensure absolute URL
                    full_url = urljoin(base_url, file_url)
                    
                    # Extract filename from URL
                    filename = os.path.basename(full_url)
                    
                    # Construct save path
                    save_path = os.path.join(output_dir, filename)

                    try:
                        print(f"Downloading: {filename}")
                        file_response = requests.get(full_url)
                        file_response.raise_for_status()
                        
                        with open(save_path, 'wb') as f:
                            f.write(file_response.content)
                        
                        print(f"Saved to: {save_path}")
                        download_count += 1
                    except Exception as e:
                        print(f"Failed to download {full_url}: {e}")

        print(f"\nDownload complete. Total files downloaded: {download_count}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    download_stock_data()
