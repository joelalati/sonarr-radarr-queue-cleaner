# Simple Sonarr and Radarr script created by Matt (MattDGTL) Pomales to clean out stalled downloads.
# Coulnd't find a python script to do this job so I figured why not give it a try.

import os
import asyncio
import logging
import requests
from requests.exceptions import RequestException
import json

# Set up logging
logging.basicConfig(
    format='%(asctime)s [%(levelname)s]: %(message)s', 
    level=logging.INFO, 
    handlers=[logging.StreamHandler()]
)

# Sonarr and Radarr API endpoints
SONARR_API_URL = (os.environ['SONARR_URL']) + "/api/v3"
RADARR_API_URL = (os.environ['RADARR_URL']) + "/api/v3"

# API key for Sonarr and Radarr
SONARR_API_KEY = (os.environ['SONARR_API_KEY'])
RADARR_API_KEY = (os.environ['RADARR_API_KEY'])

# Timeout for API requests in seconds
API_TIMEOUT = int(os.environ['API_TIMEOUT']) # 10 minutes

# Dictionary to store strikes for each item
strikes = {}

# Function to make API requests with error handling
async def make_api_request(url, api_key, params=None):
    try:
        headers = {'X-Api-Key': api_key}
        response = await asyncio.get_event_loop().run_in_executor(None, lambda: requests.get(url, params=params, headers=headers))
        response.raise_for_status()
        return response.json()
    except RequestException as e:
        logging.error(f'Error making API request to {url}: {e}')
        return None
    except ValueError as e:
        logging.error(f'Error parsing JSON response from {url}: {e}')
        return None

# Function to make API delete with error handling
async def make_api_delete(url, api_key, params=None):
    try:
        headers = {'X-Api-Key': api_key}
        response = await asyncio.get_event_loop().run_in_executor(None, lambda: requests.delete(url, params=params, headers=headers))
        response.raise_for_status()
        return response.json()
    except RequestException as e:
        logging.error(f'Error making API delete request to {url}: {e}')
        return None
    except ValueError as e:
        logging.error(f'Error parsing JSON response from {url}: {e}')
        return None

# Function to check the status of items and update strikes
async def check_stalled_items():
    global strikes
    # Example function to get stalled items from Sonarr and Radarr
    stalled_items = await get_stalled_items()
    
    for item in stalled_items:
        item_id = item['id']
        if item['status'] == 'stalled':
            if item_id in strikes:
                strikes[item_id] += 1
            else:
                strikes[item_id] = 1
            
            if strikes[item_id] >= 6:
                await blacklist_remove_search(item_id)
                strikes.pop(item_id)
        else:
            if item_id in strikes:
                strikes[item_id] = 0

# Example function to get stalled items from Sonarr and Radarr
async def get_stalled_items():
    # Implement the logic to get stalled items from Sonarr and Radarr
    return []

# Example function to blacklist, remove, and search an item
async def blacklist_remove_search(item_id):
    # Implement the logic to blacklist, remove, and search an item
    pass

# Schedule the check to run every 10 minutes
async def main():
    while True:
        await check_stalled_items()
        await asyncio.sleep(600)  # Sleep for 10 minutes

if __name__ == "__main__":
    asyncio.run(main())
