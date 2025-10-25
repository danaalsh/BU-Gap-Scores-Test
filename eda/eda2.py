import requests
import json
import time
import sys
import pandas as pd
from datetime import datetime

EMAIL = "nakyeongkim@bu.edu" 
BASE_URL = "https://api.openalex.org/works"
SEARCH_TERM = ""
OUTPUT_FILENAME = "all_works_2020_2025.csv"

# Limit to 2020-2025
START_YEAR = 2020
END_YEAR = 2025
YEARS_FILTER = f'{START_YEAR}-{END_YEAR}'

# API Limits
RESULTS_PER_PAGE = 200 
MAX_RETRIES = 3


def make_api_request(url, params, attempt=0):
    """Handles API request, retries, and error checking."""
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, params=params)
            response.raise_for_status() 
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            status_code = response.status_code
            if status_code == 429:
                wait_time = 2 ** attempt
                print(f"\n   Rate limit hit (429). Waiting {wait_time} seconds before retrying...")
                time.sleep(wait_time)
            elif status_code == 403:
                print(f"\n   Fatal HTTP Error: 403 Forbidden. The email '{EMAIL}' or your network IP is blocked.")
                return None
            elif attempt < MAX_RETRIES - 1:
                 print(f"   HTTP Error: {e}. Retrying...")
                 time.sleep(1)
            else:
                print(f"\n   Fatal HTTP Error after all retries: {e}")
                return None
        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                print(f"   Connection Error: {e}. Retrying...")
                time.sleep(2)
            else:
                print(f"\n   Fatal Connection Error after all retries: {e}")
                return None
    return None

def fetch_all_works_data():
    """Fetches all works page-by-page using cursor pagination."""
    
    # Define columns of the dataset
    SELECT_FIELDS = [
        'title', 
        'publication_date', 
        'cited_by_count',
        'concepts', 
        'authorships' 
    ]
    
    # Base parameters
    params = {
        'search': SEARCH_TERM,
        'filter': f'publication_year:{YEARS_FILTER}',
        'per-page': RESULTS_PER_PAGE,
        'select': ','.join(SELECT_FIELDS),
        'mailto': EMAIL,
        'cursor': '*' # to bypass the page limit
    }

    all_works = []
    next_cursor = '*'
    page_count = 0
    total_works_expected = 0

    while next_cursor:
        page_count += 1
        params['cursor'] = next_cursor
        
        sys.stdout.write(f" Fetching Page {page_count}...")
        sys.stdout.flush()

        data = make_api_request(BASE_URL, params)
        
        if data is None:
            print("\nFatal error encountered. Stopping fetch process.")
            return None

        # Extract works and update cursor
        results = data.get('results', [])
        meta = data.get('meta', {})
        
        # Get the total count once
        if page_count == 1:
            total_works_expected = meta.get('count', 0)
            print(f" Found {total_works_expected} total works matching criteria.")

        all_works.extend(results)
        next_cursor = meta.get('next_cursor')

        sys.stdout.write(f" Downloaded {len(results)} works. Total collected: {len(all_works)}\n")
        
        # Add a small delay to be polite to the API
        time.sleep(0.5) 
    
    return all_works

def process_and_structure_data(raw_works):
    """Processes raw works data into the required CSV structure."""
    
    final_data = []

    for work in raw_works:
        # Extract Concept (Top Concept) 
        concepts = work.get('concepts', [])
        top_concept = concepts[0] if concepts else {'display_name': 'N/A', 'id': 'N/A'}
        
        # Extract Author/Institution 
        authorships = work.get('authorships', [])
        first_authorship = authorships[0] if authorships else {}
        
        # Extract author name
        author_name = first_authorship.get('author', {}).get('display_name', 'N/A')
        
        # Extract institution name (takes the first institution if multiple are listed)
        institutions = first_authorship.get('institutions', [])
        institution_name = institutions[0].get('display_name', 'N/A') if institutions else 'N/A'
        
        # Build the final row 
        final_data.append({
            'Concept_ID': top_concept.get('id', 'N/A').split('/')[-1], # Strip URL to just the key
            'Concept_Name': top_concept.get('display_name', 'N/A'),
            'Work_Title': work.get('title', 'N/A'),
            'Primary_Author': author_name,
            'Affiliation_Institution': institution_name,
            'Citation_Count': work.get('cited_by_count', 0),
            'Publication_Date': work.get('publication_date', 'N/A')
        })
        
    return final_data

def main():
    """data fetching, processing, and CSV export."""
    
    print("=" * 70)
    print(f"Search Term: '{SEARCH_TERM}' | Years: {START_YEAR}-{END_YEAR}")
    print("=" * 70)
    
    # Fetch all raw data
    raw_works_data = fetch_all_works_data()
    
    if raw_works_data is None:
        sys.exit(1)

    # Process and Save
    if raw_works_data:
        processed_data = process_and_structure_data(raw_works_data)
        df = pd.DataFrame(processed_data)
        
        # Sort for clean display
        df['Publication_Date'] = pd.to_datetime(df['Publication_Date'], errors='coerce')
        df = df.sort_values(by=['Publication_Date', 'Citation_Count'], ascending=[False, False])
        
        df.to_csv(OUTPUT_FILENAME, index=False)
        
        print("\n" + "=" * 70)
        print(f"Analysis Complete.")
        print(f"Data saved to: {OUTPUT_FILENAME}")
        print(f"Total works processed: {len(df)}")
        print("=" * 70)
        
        # Display a preview 
        print("\n--- CSV Data Preview (Top 5 Rows) ---")
        print(df[['Publication_Date', 'Work_Title', 'Primary_Author', 'Citation_Count']].head().to_string(index=False))
        print("--------------------------------------")
            
    else:
        print("\n Failed to collect any data for analysis.")

if __name__ == "__main__":
    main()