import asyncio
import logging
import json
import requests
from typing import Optional, Any
from datetime import datetime, timedelta

# --- Configuration Loading ---
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

# --- Global Dictionaries for Tracking Download States ---
strike_counts = {} # For "stalled with no connections"
download_progress_tracking = {} # For non-progressing download detection using 'sizeleft'

# --- Constants for Non-Progressing Download Detection ---
# These define how many checks (API_TIMEOUT intervals) a download must show no progress
# in its 'sizeleft' before it's considered stuck and deleted.
NO_PROGRESS_THRESHOLD_BYTES = 1024 * 1024 # 1 MB - minimum change in sizeleft to count as progress
NO_PROGRESS_STRIKE_COUNT = 3             # Number of consecutive checks with no significant progress before deleting

# --- Global variable for weekly Sonarr search ---
# Initialize with a past timestamp to trigger search on first run
last_sonarr_weekly_search_timestamp = 0.0 # Unix timestamp of last search

# --- Logging Setup ---
logging.basicConfig(
    format='%(asctime)s [%(levelname)s]: %(message)s',
    level=logging.INFO,
    handlers=[logging.StreamHandler()]
)

# --- API Request Functions ---
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

# --- Helper Functions ---
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

def _delete_and_blocklist_item(item_id: int, title: str, api_url: str, api_key: str, queue_name: str, reason: str) -> bool:
    """
    Helper function to delete and blocklist a queue item, and clean up its tracking info.

    Args:
        item_id (int): The ID of the item to delete.
        title (str): The title of the item for logging.
        api_url (str): The base API URL (Sonarr/Radarr).
        api_key (str): The API key.
        queue_name (str): 'Sonarr' or 'Radarr'.
        reason (str): The reason for deletion (e.g., 'failed', 'dangerous file', 'stalled').

    Returns:
        bool: True if deletion was successful, False otherwise.
    """
    logging.warning(f'Found {reason} download: "{title}" (ID: {item_id}). Deleting and blocklisting.')
    delete_result = make_api_delete(
        f'{api_url}/queue/{item_id}',
        api_key,
        {'removeFromClient': 'true', 'blocklist': 'true'}
    )
    if delete_result:
        logging.info(f'Successfully deleted and blocklisted {queue_name} item ({reason}): {title}')
        # Clean up tracking info for this item across all systems
        if item_id in strike_counts:
            del strike_counts[item_id]
        if item_id in download_progress_tracking:
            del download_progress_tracking[item_id]
        return True
    else:
        logging.error(f'Failed to delete and blocklist {queue_name} item ({reason}): {title}')
        return False

def _trigger_search_command(item: dict, api_url: str, api_key: str, is_sonarr: bool, title: str, queue_name: str) -> None:
    """
    Helper function to trigger a re-search command for a series or movie.

    Args:
        item (dict): The queue item dictionary.
        api_url (str): The base API URL (Sonarr/Radarr).
        api_key (str): The API key.
        is_sonarr (bool): True if Sonarr, False if Radarr.
        title (str): The title of the item for logging.
        queue_name (str): 'Sonarr' or 'Radarr'.
    """
    search_payload = {}
    if is_sonarr:
        series_id = item.get('seriesId')
        if series_id:
            search_payload = {"name": "SeriesSearch", "seriesId": series_id}
        else:
            logging.warning(f'Could not find SeriesId for re-search for Sonarr item: {title} (ID: {item.get("id")})')
    else: # Radarr
        movie_id = item.get('movieId')
        if movie_id:
            search_payload = {"name": "MovieSearch", "movieId": movie_id}
        else:
            logging.warning(f'Could not find MovieId for re-search for Radarr item: {title} (ID: {item.get("id")})')

    if search_payload:
        logging.debug(f"Attempting to trigger search with payload: {search_payload}")
        search_result = make_api_post(f'{api_url}/command', api_key, search_payload)
        if search_result:
            logging.info(f'Successfully triggered re-search for {queue_name} item: {title}')
        else:
            logging.error(f'Failed to trigger re-search for {queue_name} item: {title}')
    else:
        logging.warning(f'No valid search payload generated for {queue_name} item: {title}')

# --- Main Queue Processing Logic ---
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
        status_messages = item.get("statusMessages")
        current_sizeleft = item.get('sizeleft')
        protocol = item.get('protocol') # Get the protocol (usenet or torrent)

        # Basic validation for essential keys
        if not all(k in item for k in ['id', 'title', 'status', 'trackedDownloadStatus', 'trackedDownloadState']):
            logging.warning(f'Skipping item in {queue_name} queue due to missing essential keys: {item.keys()}')
            continue

        # Extract all messages from statusMessages for checking
        all_status_messages_text = []
        if isinstance(status_messages, list):
            for sm_entry in status_messages:
                # Add the 'title' of the status message entry
                if isinstance(sm_entry, dict) and "title" in sm_entry:
                    all_status_messages_text.append(sm_entry["title"])
                # Add messages from the 'messages' list within the entry
                if isinstance(sm_entry, dict) and "messages" in sm_entry and isinstance(sm_entry["messages"], list):
                    all_status_messages_text.extend(sm_entry["messages"])

        logging.info(f'Processing item: {title} (ID: {item_id}) - Status: {status}, Tracked Status: {tracked_download_status}, Tracked State: {tracked_download_state}, Error: {error_message}, Status Messages: {all_status_messages_text}, Size Left: {current_sizeleft}, Protocol: {protocol}')

        # --- Handle "Failed" downloads ---
        if status == 'failed':
            _delete_and_blocklist_item(item_id, title, api_url, api_key, queue_name, "failed")
            continue # Move to the next item

        # --- Handle "One or more movies/episodes expected in this release were not imported or missing" ---
        # This applies to both Sonarr and Radarr
        is_missing_files_warning = any("not imported or missing" in msg for msg in all_status_messages_text)

        if is_missing_files_warning and \
           status == "completed" and \
           tracked_download_status == "warning" and \
           tracked_download_state == "importPending":
            
            logging.warning(f'{queue_name} item: "{title}" (ID: {item_id}) indicates missing files. Deleting and blocklisting (no re-search).')
            _delete_and_blocklist_item(item_id, title, api_url, api_key, queue_name, "missing files")
            continue # Move to the next item

        # --- Handle "No files found are eligible for import" ---
        is_no_eligible_files_warning = any("No files found are eligible for import" in msg for msg in all_status_messages_text)

        if is_no_eligible_files_warning and \
           status == "completed" and \
           tracked_download_status == "warning" and \
           tracked_download_state == "importPending":

            logging.warning(f'No eligible files found for import for {queue_name} item: {title} (ID: {item_id}). Deleting, blocklisting, and re-searching.')

            if _delete_and_blocklist_item(item_id, title, api_url, api_key, queue_name, "no eligible files"):
                _trigger_search_command(item, api_url, api_key, is_sonarr, title, queue_name)
            continue # Move to the next item

        # --- Handle "Potentially dangerous file" with specific tracked status/state ---
        is_dangerous_file_warning = any("Caution: Found potentially dangerous file" in msg for msg in all_status_messages_text)

        if is_dangerous_file_warning and \
           tracked_download_status == "warning" and \
           tracked_download_state == "importPending":

            logging.warning(f'Potentially dangerous file found for {queue_name} item: {title} (ID: {item_id}). Deleting, blocklisting, and re-searching.')

            if _delete_and_blocklist_item(item_id, title, api_url, api_key, queue_name, "potentially dangerous file"):
                _trigger_search_command(item, api_url, api_key, is_sonarr, title, queue_name)
            continue # Move to the next item

        # --- Handle general "importBlocked" warnings (applies to both Sonarr and Radarr) ---
        # This will now cover cases where trackedDownloadState is 'importBlocked',
        # including scenarios like "Not an upgrade" if the API reports it as 'importBlocked'.
        if status == "completed" and \
           tracked_download_status == "warning" and \
           tracked_download_state == "importBlocked":
            
            logging.warning(f'{queue_name} item: "{title}" (ID: {item_id}) is completed with a warning and import is blocked. Deleting and blocklisting (no re-search).')
            _delete_and_blocklist_item(item_id, title, api_url, api_key, queue_name, "import blocked")
            continue # Move to the next item

        # --- Handle "Stalled with no connections" error ---
        if status == 'warning' and error_message == 'The download is stalled with no connections':
            item_id = item['id']
            if item_id not in strike_counts:
                strike_counts[item_id] = 0
            strike_counts[item_id] += 1
            logging.info(f'Item "{title}" has {strike_counts[item_id]} connection stalls.')
            if strike_counts[item_id] >= STRIKE_COUNT:
                _delete_and_blocklist_item(item_id, title, api_url, api_key, queue_name, "stalled")
            continue # Move to the next item
        elif item_id in strike_counts:
            # Item is no longer stalled by connection issues, reset its strike count
            logging.info(f'Item "{title}" is no longer connection stalled. Resetting strike count.')
            del strike_counts[item_id]

        # --- Handle downloads stuck in "downloading" using 'sizeleft' (Only for torrents) ---
        if status == 'downloading' and current_sizeleft is not None and protocol == 'torrent':
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
                        _delete_and_blocklist_item(item_id, title, api_url, api_key, queue_name, "non-progressing")
        elif item_id in download_progress_tracking:
            # Item is no longer 'downloading', sizeleft is missing, or it's not a torrent, remove from tracking
            logging.debug(f"Download {title} (ID: {item_id}) is no longer downloading, missing sizeleft, or is not a torrent. Removing from tracking.")
            del download_progress_tracking[item_id]

        # --- General Fallback for Completed with Warning/Error Status ---
        # This acts as a catch-all for items that completed downloading but have
        # a warning or error tracked status and weren't handled by more specific rules above.
        # This is intentionally placed last among all the status checks that might lead to a 'continue'.
        if status == "completed" and \
           (tracked_download_status == "warning" or tracked_download_status == "error"):

            logging.warning(f'{queue_name} item: "{title}" (ID: {item_id}) completed with a general warning/error and was not specifically handled. Deleting, blocklisting, and re-searching.')
            if _delete_and_blocklist_item(item_id, title, api_url, api_key, queue_name, "general completed warning/error"):
                _trigger_search_command(item, api_url, api_key, is_sonarr, title, queue_name)
            continue # Move to the next item


# --- Wrapper Functions for Queue Processing ---
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

# --- Main Execution Loop ---
async def main() -> None:
    """
    Main function to run the queue cleaner script periodically.
    """
    global last_sonarr_weekly_search_timestamp # Declare global to modify it

    while True:
        logging.info('Running media-tools script')
        await remove_stalled_sonarr_downloads()
        await remove_stalled_radarr_downloads()
        
        # --- Weekly Sonarr Wanted Episodes Search ---
        current_time = datetime.now().timestamp() # Get current Unix timestamp
        one_week_in_seconds = 7 * 24 * 60 * 60

        if (current_time - last_sonarr_weekly_search_timestamp) >= one_week_in_seconds:
            logging.info('It\'s been a week since the last Sonarr wanted episodes search. Triggering MissingEpisodeSearch command.')
            search_command = {"name": "MissingEpisodeSearch"} 
            search_result = make_api_post(f'{SONARR_API_URL}/command', SONARR_API_KEY, search_command)
            if search_result:
                logging.info('Successfully triggered Sonarr MissingEpisodeSearch for wanted episodes.')
                last_sonarr_weekly_search_timestamp = current_time # Update timestamp
            else:
                logging.error('Failed to trigger Sonarr MissingEpisodeSearch. Check Sonarr logs for details.')

        # Log current strike counts and download progress tracking
        if strike_counts:
            logging.info(f'Current connection strike counts: {strike_counts}')
        else:
            logging.info('No items currently have connection strikes.')
            
        if download_progress_tracking:
            logging.info(f'Current non-progressing download tracking: {download_progress_tracking}')
        else:
            logging.info('No items currently being tracked for non-progressing downloads.')

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
