import asyncio
import logging
import json
import requests
from typing import Optional, Any # Import Optional and Any for type hinting
from datetime import datetime # Import datetime for parsing timestamps (still useful for other time-based checks if needed)

# Load configuration from config.json
try:
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
except FileNotFoundError:
    logging.error("config.json not found. Please create it with your API URLs and keys.")
    exit(1)
except json.JSONDecodeError:
    logging.error("Error decoding config.json. Please check its format.")
    exit(1)

# Retrieve configuration values, providing default empty strings or values if not found
SONARR_API_URL = config.get('SONARR_API_URL', '') + "/api/v3"
SONARR_API_KEY = config.get('SONARR_API_KEY', '')
RADARR_API_URL = config.get('RADARR_API_URL', '') + "/api/v3"
RADARR_API_KEY = config.get('RADARR_API_KEY', '')
API_TIMEOUT = config.get('API_TIMEOUT', 300) # Default to 300 seconds (5 minutes) for API calls and sleep
STRIKE_COUNT = config.get('STRIKE_COUNT', 3) # Default to 3 strikes for "no connections" stalls

# Global dictionaries for tracking various download states
strike_counts = {} # For "stalled with no connections"

# For non-progressing download detection using 'sizeleft'
# Stores the last observed 'sizeleft' and a counter for consecutive checks with no progress
download_progress_tracking = {}

# Constants for non-progressing download detection using 'sizeleft'
# These define how many checks (API_TIMEOUT intervals) a download must show no progress
# in its 'sizeleft' before it's considered stuck and deleted.
NO_PROGRESS_THRESHOLD_BYTES = 1024 * 1024 # 1 MB - minimum change in sizeleft to count as progress
NO_PROGRESS_STRIKE_COUNT = 3             # Number of consecutive checks with no significant progress before deleting

# Set up logging
logging.basicConfig(
    format='%(asctime)s [%(levelname)s]: %(message)s',
    level=logging.INFO,
    handlers=[logging.StreamHandler()]
)

# Function to make API requests (GET)
def make_api_request(url: str, api_key: str, params: Optional[dict] = None) -> Optional[Any]:
    """
    Makes a GET API request to the specified URL.

    Args:
        url (str): The URL for the API endpoint.
        api_key (str): The API key for authentication.
        params (Optional[dict]): Optional dictionary of query parameters.

    Returns:
        Optional[Any]: The JSON response from the API if successful, otherwise None.
    """
    headers = {
        'X-Api-Key': api_key
    }
    try:
        response = requests.get(url, headers=headers, params=params, timeout=API_TIMEOUT)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        return response.json()
    except requests.exceptions.Timeout:
        logging.error(f'API request to {url} timed out after {API_TIMEOUT} seconds.')
        return None
    except requests.exceptions.RequestException as e:
        logging.error(f'Error making API request to {url}: {e}')
        return None

# Function to make API delete requests (DELETE)
def make_api_delete(url: str, api_key: str, params: Optional[dict] = None) -> Optional[Any]:
    """
    Makes a DELETE API request to the specified URL.

    Args:
        url (str): The URL for the API endpoint.
        api_key (str): The API key for authentication.
        params (Optional[dict]): Optional dictionary of query parameters.

    Returns:
        Optional[Any]: The JSON response from the API if successful, otherwise None.
    """
    headers = {
        'X-Api-Key': api_key
    }
    try:
        response = requests.delete(url, headers=headers, params=params, timeout=API_TIMEOUT)
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        # Some DELETE endpoints might not return JSON, so handle that gracefully
        try:
            return response.json()
        except json.JSONDecodeError:
            logging.info(f"DELETE request to {url} returned non-JSON response (Status: {response.status_code}).")
            return {"status": "success", "message": "No JSON response from delete operation"}
    except requests.exceptions.Timeout:
        logging.error(f'API delete request to {url} timed out after {API_TIMEOUT} seconds.')
        return None
    except requests.exceptions.RequestException as e:
        logging.error(f'Error making API delete request to {url}: {e}')
        return None

# Function to make API post requests (POST)
def make_api_post(url: str, api_key: str, data: Optional[dict] = None) -> Optional[Any]:
    """
    Makes a POST API request to the specified URL. Used for sending commands like search.

    Args:
        url (str): The URL for the API endpoint.
        api_key (str): The API key for authentication.
        data (Optional[dict]): Optional dictionary of data to send in the request body (JSON).

    Returns:
        Optional[Any]: The JSON response from the API if successful, otherwise None.
    """
    headers = {
        'X-Api-Key': api_key,
        'Content-Type': 'application/json'
    }
    try:
        response = requests.post(url, headers=headers, json=data, timeout=API_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.Timeout:
        logging.error(f'API POST request to {url} timed out after {API_TIMEOUT} seconds.')
        return None
    except requests.exceptions.RequestException as e:
        logging.error(f'Error making API POST request to {url}: {e}')
        return None

# Function to count records in a queue
def count_records(api_url: str, api_key: str) -> int:
    """
    Counts the total number of records in a given API queue.

    Args:
        api_url (str): The base URL for the API (Sonarr/Radarr).
        api_key (str): The API key for authentication.

    Returns:
        int: The total number of records, or 0 if the queue cannot be retrieved.
    """
    the_url = f'{api_url}/queue'
    the_queue = make_api_request(the_url, api_key)
    if the_queue is not None and isinstance(the_queue, dict) and 'totalRecords' in the_queue:
        return the_queue['totalRecords']
    logging.warning(f"Could not retrieve total records for {api_url}/queue. Returning 0.")
    return 0

async def process_queue(api_url: str, api_key: str, is_sonarr: bool = True) -> None:
    """
    Processes the Sonarr or Radarr queue to identify and act on stalled,
    dangerous, or non-progressing downloads.

    Args:
        api_url (str): The base URL for the API (Sonarr/Radarr).
        api_key (str): The API key for authentication.
        is_sonarr (bool): True if processing Sonarr queue, False for Radarr.
    """
    queue_name = "Sonarr" if is_sonarr else "Radarr"
    logging.info(f'Checking {queue_name} queue for stalled, dangerous, and non-progressing items...')

    total_records = count_records(api_url, api_key)
    if total_records == 0:
        logging.info(f"{queue_name} queue is empty or could not be retrieved. Skipping processing.")
        return

    queue_url = f'{api_url}/queue'
    queue_data = make_api_request(queue_url, api_key, {'page': '1', 'pageSize': total_records})

    if queue_data is None or 'records' not in queue_data or not isinstance(queue_data['records'], list):
        logging.warning(f'{queue_name} queue data is invalid or empty. Skipping processing.')
        return

    logging.info(f'Processing {len(queue_data["records"])} items in {queue_name} queue...')
    for item in queue_data['records']:
        item_id = item.get('id')
        title = item.get('title', 'Unknown Item')
        status = item.get('status')
        tracked_download_status = item.get('trackedDownloadStatus')
        tracked_download_state = item.get('trackedDownloadState')
        error_message = item.get('errorMessage')
        status_messages = item.get("statusMessages") # Correctly get statusMessages
        current_sizeleft = item.get('sizeleft') # Get current sizeleft

        # Basic validation for essential keys
        if not all(k in item for k in ['id', 'title', 'status', 'trackedDownloadStatus', 'trackedDownloadState']):
            logging.warning(f'Skipping item in {queue_name} queue due to missing essential keys: {item.keys()}')
            continue

        # Extract all messages from statusMessages for checking
        all_status_messages_text = []
        if isinstance(status_messages, list):
            for sm_entry in status_messages:
                if isinstance(sm_entry, dict) and "messages" in sm_entry and isinstance(sm_entry["messages"], list):
                    all_status_messages_text.extend(sm_entry["messages"])

        logging.info(f'Processing item: {title} (ID: {item_id}) - Status: {status}, Tracked Status: {tracked_download_status}, Tracked State: {tracked_download_state}, Error: {error_message}, Status Messages: {all_status_messages_text}, Size Left: {current_sizeleft}')

        # --- Handle "Potentially dangerous file" with specific tracked status/state ---
        # This logic now applies to both Sonarr and Radarr
        is_dangerous_file_warning = any("Caution: Found potentially dangerous file" in msg for msg in all_status_messages_text)
        
        # Check if the item is in a 'warning' state and 'importPending' state
        # and has the dangerous file warning.
        if is_dangerous_file_warning and \
           tracked_download_status == "warning" and \
           tracked_download_state == "importPending":

            logging.warning(f'Potentially dangerous file found for {queue_name} item: {title} (ID: {item_id}). Deleting, blocklisting, and re-searching.')

            # Delete and blocklist the item
            delete_result = make_api_delete(
                f'{api_url}/queue/{item_id}',
                api_key,
                {'removeFromClient': 'true', 'blocklist': 'true'}
            )
            if delete_result:
                logging.info(f'Successfully deleted and blocklisted {queue_name} item (dangerous file): {title}')

                # Trigger re-search for the series/movie
                search_payload = {}
                if is_sonarr:
                    series_id = item.get('seriesId')
                    if series_id:
                        search_payload = {"name": "SeriesSearch", "seriesId": series_id}
                    else:
                        logging.warning(f'Could not find SeriesId for re-search for Sonarr item: {title} (ID: {item_id})')
                else: # Radarr
                    movie_id = item.get('movieId') # Radarr queue items should have 'movieId'
                    if movie_id:
                        search_payload = {"name": "MovieSearch", "movieId": movie_id}
                    else:
                        logging.warning(f'Could not find MovieId for re-search for Radarr item: {title} (ID: {item_id})')

                if search_payload:
                    logging.debug(f"Attempting to trigger search with payload: {search_payload}") # Added for debugging 500 error
                    search_result = make_api_post(f'{api_url}/command', api_key, search_payload)
                    if search_result:
                        logging.info(f'Successfully triggered re-search for {queue_name} item: {title}')
                    else:
                        logging.error(f'Failed to trigger re-search for {queue_name} item: {title}')
                else:
                    logging.warning(f'No valid search payload generated for {queue_name} item: {title}')

                # Clean up tracking info for this item across all systems
                if item_id in strike_counts: del strike_counts[item_id]
                if item_id in download_progress_tracking: del download_progress_tracking[item_id]
            else:
                logging.error(f'Failed to delete and blocklist {queue_name} item (dangerous file): {title}')
            continue # Move to the next item in the queue

        # --- Handle "Stalled with no connections" error (applies to both Sonarr and Radarr) ---
        if status == 'warning' and error_message == 'The download is stalled with no connections':
            item_id = item['id']
            if item_id not in strike_counts:
                strike_counts[item_id] = 0
            strike_counts[item_id] += 1
            logging.info(f'Item "{title}" has {strike_counts[item_id]} connection stalls.')
            if strike_counts[item_id] >= STRIKE_COUNT:
                logging.info(f'Deleting and blocklisting stalled download: "{title}" (ID: {item_id})')
                delete_result = make_api_delete(f'{api_url}/queue/{item_id}', api_key, {'removeFromClient': 'true', 'blocklist': 'true'})
                if delete_result:
                    logging.info(f'Successfully deleted and blocklisted stalled download: "{title}"')
                    # Clean up tracking info for this item
                    if item_id in strike_counts: del strike_counts[item_id]
                    if item_id in download_progress_tracking: del download_progress_tracking[item_id]
                else:
                    logging.error(f'Failed to delete and blocklist stalled download: "{title}"')
            continue # Move to the next item
        elif item_id in strike_counts:
            # Item is no longer stalled by connection issues, reset its strike count
            logging.info(f'Item "{title}" is no longer connection stalled. Resetting strike count.')
            del strike_counts[item_id]

        # --- Handle downloads stuck in "downloading" using 'sizeleft' ---
        if status == 'downloading' and current_sizeleft is not None:
            if item_id not in download_progress_tracking:
                # First time seeing this item in 'downloading' status, initialize tracking
                download_progress_tracking[item_id] = {
                    'last_sizeleft': current_sizeleft,
                    'no_progress_count': 0
                }
                logging.debug(f"Started tracking download progress for {title} (ID: {item_id}). Initial size left: {current_sizeleft}.")
            else:
                tracking_info = download_progress_tracking[item_id]
                last_sizeleft = tracking_info['last_sizeleft']

                # Check if progress has been made (current_sizeleft is significantly less than last_sizeleft)
                if (last_sizeleft - current_sizeleft) > NO_PROGRESS_THRESHOLD_BYTES:
                    # Progress made, reset counter and update last_sizeleft
                    tracking_info['last_sizeleft'] = current_sizeleft
                    tracking_info['no_progress_count'] = 0
                    logging.debug(f"Download {title} (ID: {item_id}) is progressing. Size left: {current_sizeleft}.")
                else:
                    # No significant progress, increment no_progress_count
                    tracking_info['no_progress_count'] += 1
                    logging.warning(f'Download "{title}" (ID: {item_id}) has shown no significant progress. No progress count: {tracking_info["no_progress_count"]}. Size left: {current_sizeleft}.')

                    if tracking_info['no_progress_count'] >= NO_PROGRESS_STRIKE_COUNT:
                        logging.info(f'Deleting and blocklisting non-progressing download (sizeleft stuck): "{title}" (ID: {item_id})')
                        delete_result = make_api_delete(f'{api_url}/queue/{item_id}', api_key, {'removeFromClient': 'true', 'blocklist': 'true'})
                        if delete_result:
                            logging.info(f'Successfully deleted and blocklisted non-progressing download: "{title}"')
                            # Clean up tracking info for this item
                            del download_progress_tracking[item_id]
                        else:
                            logging.error(f'Failed to delete and blocklist non-progressing download: "{title}"')
        elif item_id in download_progress_tracking:
            # Item is no longer 'downloading' or sizeleft is missing, remove from tracking
            logging.debug(f"Download {title} (ID: {item_id}) is no longer downloading or missing sizeleft. Removing from tracking.")
            del download_progress_tracking[item_id]


async def remove_stalled_sonarr_downloads() -> None:
    """
    Wrapper function to call the unified queue processing for Sonarr.
    """
    await process_queue(SONARR_API_URL, SONARR_API_KEY, is_sonarr=True)

async def remove_stalled_radarr_downloads() -> None:
    """
    Wrapper function to call the unified queue processing for Radarr.
    """
    await process_queue(RADARR_API_URL, RADARR_API_KEY, is_sonarr=False)

# Main function
async def main() -> None:
    """
    Main function to run the queue cleaner script periodically.
    """
    while True:
        logging.info('Running media-tools script')
        await remove_stalled_sonarr_downloads()
        await remove_stalled_radarr_downloads()
        logging.info(f'Finished running media-tools script. Sleeping for {API_TIMEOUT / 60} minutes.')
        await asyncio.sleep(API_TIMEOUT)

if __name__ == '__main__':
    # Add a check for API keys and URLs before starting the loop
    if not (SONARR_API_URL and SONARR_API_KEY and RADARR_API_URL and RADARR_API_KEY):
        logging.critical("One or more API URLs or Keys are missing in config.json. Please ensure they are set.")
        exit(1)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Script terminated by user (Ctrl+C). Exiting.")
    except Exception as e:
        logging.critical(f"An unhandled error occurred: {e}", exc_info=True)
        exit(1)
