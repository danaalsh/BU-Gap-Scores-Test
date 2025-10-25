import requests
import json
import time
import sys
import pandas as pd
from datetime import datetime

EMAIL = "nakyeongkim@bu.edu" 

BASE_URL = "https://api.openalex.org/works"
SEARCH_TERM = "generative AI"

RESULTS_PER_PAGE = 50
MAX_RETRIES = 3

CURRENT_YEAR = datetime.now().year
START_YEAR = CURRENT_YEAR - 5
YEARS_TO_ANALYZE = list(range(START_YEAR, CURRENT_YEAR + 1))

def make_api_request(url, params):
    for attempt in range(MAX_RETRIES):
        try:
            # Make the API request
            response = requests.get(url, params=params)
            response.raise_for_status() # Raise exception for 4xx or 5xx status codes
            
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            status_code = response.status_code
            if status_code == 429:
                wait_time = 2 ** attempt
                time.sleep(wait_time)
            elif status_code == 403:
                # Critical handling for the Forbidden error
                print(f"\n   Fatal HTTP Error: 403 Forbidden. Your request was rejected by OpenAlex.")
                return None
            elif attempt < MAX_RETRIES - 1:
                 print(f"\n   HTTP Error: {e}.")
                 time.sleep(1)
            else:
                print(f"\n   Fatal HTTP Error after all retries: {e}")
                return None
        except requests.exceptions.RequestException as e:
            # Handle connection or timeout errors
            if attempt < MAX_RETRIES - 1:
                print(f"   Connection Error: {e}. Retrying.")
                time.sleep(2)
            else:
                print(f"\n    Fatal Connection Error after all retries: {e}")
                return None
    return None

def fetch_concepts_by_year():
    """Iterates through years, fetches concept data, and aggregates results."""
    all_data = []

    for year in YEARS_TO_ANALYZE:
        sys.stdout.write(f"Fetching data for year {year} ({SEARCH_TERM}) ")
        sys.stdout.flush()

        # Dynamic API Parameters for the current year
        params = {
            'search': SEARCH_TERM,
            'group-by': 'concepts.id',
            'filter': f'publication_year:{year}',
            'per-page': RESULTS_PER_PAGE,
            'mailto': EMAIL
        }

        data = make_api_request(BASE_URL, params)
        
        if data and 'group_by' in data:
            groups = data['group_by']
            
            # Process and store each concept group
            for group in groups:
                all_data.append({
                    'Year': year,
                    'Concept_ID': group.get('key', 'N/A'),
                    'Concept_Name': group.get('key_display_name', 'N/A'),
                    'Work_Count': group.get('count', 0)
                })
            sys.stdout.write(f"Found {len(groups)} top concepts. (Total works: {data['meta'].get('count', 'N/A')})\n")
        else:
            sys.stdout.write("Skipped.\n")
            # If a fatal error occurred (e.g., 403), stop the entire process
            if data is None:
                return None 

    return all_data

def main():
    """Orchestrates the data fetching, processing, and CSV export."""

    print("=" * 70)
    print(f"OpenAlex Cross-Sectional Analysis")
    print(f"Search Term: '{SEARCH_TERM}' | Years: {START_YEAR}-{CURRENT_YEAR}")
    print("=" * 70)

    # Fetch Data
    raw_data = fetch_concepts_by_year()
    
    if raw_data is None:
        # A fatal error (like 403) occurred inside the fetch function
        sys.exit(1)

    # Process and Save
    if raw_data:
        df = pd.DataFrame(raw_data)
        output_filename = 'concept_by_year.csv'
        
        # Sort for clean presentation
        df = df.sort_values(by=['Year', 'Work_Count'], ascending=[True, False])
        
        df.to_csv(output_filename, index=False)
        
        print("\n" + "=" * 70)
        print(f"Total rows (Concept-Year combinations): {len(df)}")
        print("=" * 70)
        
        # Display a preview of the saved data
        print("\n--- CSV Data Preview (Top 5 Rows) ---")
        print(df.head().to_string(index=False))
        print("--------------------------------------")
            
    else:
        print("\n❌ Failed to collect any data for analysis.")

if __name__ == "__main__":    
    main()
