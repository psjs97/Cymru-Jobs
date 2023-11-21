# Importing required libraries
import requests
from dotenv import load_dotenv
from os import environ
import pandas as pd
from datetime import datetime
import argparse

# Load environment variables from the .env file
load_dotenv()

# Cymru domain
CYMRU_DOMAIN = environ.get('CYMRU_DOMAIN')

# Fetch API key from environment variables
CYMRU_API_KEY = environ.get('CYMRU_API_KEY')

# Function to fetch data from the Cymru API
def fetch_data(api_key, start_date=None, end_date=None):
    # Define headers for the API request
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': f'Token {CYMRU_API_KEY}',
    }

    # Create a dictionary to hold the request parameters
    params = {}

    # Add start_date to the dictionary if it has a value
    if start_date:
        params['start_date'] = datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y-%m-%dT00:00:00Z')

    # Add end_date to the dictionary if it has a value
    if end_date:
        params['end_date'] = datetime.strptime(end_date, '%Y-%m-%d').strftime('%Y-%m-%dT00:00:00Z')

    # Define the initial URL for the API endpoint
    url = f'https://{CYMRU_DOMAIN}/api/jobs'

    # Initialize an empty list to store all results
    all_results = []

    # Start a loop to retrieve data from paginated API responses
    while url:
        print(f'Getting data from {url}')

        # Make a GET request to the API with the specified headers and params
        response = requests.get(url, headers=headers, params=params)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Append the results from the current page to the list
            all_results.extend(response.json()['data'])

            # Get the URL of the next page from the API response
            url = response.json().get('next_page_url')
        else:
            # Print an error message and exit the loop if the request fails
            print(f"Error in the request: {response.status_code}")
            break

    # Create a pandas DataFrame from the collected results
    df = pd.DataFrame(all_results)

    return df
    
# Function to construct export link
def construct_url(row):
    return f'https://recon.cymru.com/jobs/{row["id"]}/export?format=csv'

# Main function where the code is executed
def main():
    # Create an argument parser
    parser = argparse.ArgumentParser(description='Fetch data Jobs from Cymru API and save it to a CSV file.')
    
    # Add start_date and end_date as command line arguments
    parser.add_argument('--start_date', nargs='?', help='Start date for fetching data (YYYY-MM-DD)')
    parser.add_argument('--end_date', nargs='?', help='End date for fetching data (YYYY-MM-DD)')
    
    # Parse the command line arguments
    args = parser.parse_args()

    # Fetch data from Cymru API using the provided API key and date parameters
    df = fetch_data(CYMRU_API_KEY, args.start_date, args.end_date)

    # Add 'execution_datetime' column with script execution
    df['execution_datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Bytes -> MBytes
    df['storage_used'] = (df['total_bytes'] / (1024 ** 2)).round(2)

    # 'updated_at' column to datetime type
    df['updated_at'] = pd.to_datetime(df['updated_at'])

    # Sort Dataframe by 'updated_at' column (last updated at the top)
    df_sorted = df.sort_values(by='updated_at', ascending=False)

    # Apply the function to each row to build the column 'link'
    df['export_link'] = df.apply(construct_url, axis=1)
    
    # Select columns
    columns = [
        'execution_datetime',
        'username',
        'name',
        'id',
        'description',
        'status', # Storage Used
        'storage_used',
        'created_at',
        'updated_at',
        'group_name', # or group_id (Group)
        'origin',
        'scheduled_interval',
        'export_link'
    ]

    df = df[columns]
    
    # Save results in .csv file
    df.to_csv('cymru_jobs.csv',sep=';',index=None)

# Call the main function to execute the code
if __name__ == "__main__":
    main()
