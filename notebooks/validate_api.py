#!/usr/bin/env python3
import requests
import json

# The base URL of your API.
# Since Uvicorn runs on port 8000 inside the container,
# and this script will likely be run from the same container or Docker network:
API_BASE_URL = "http://localhost:8000"
# If you were running this script from *outside* the Docker container (e.g., directly on your Pi
# or another machine), you would use:
# API_BASE_URL = "http://<your-pi-ip-address>:<host-port-for-api>"
# For example: API_BASE_URL = "http://192.168.0.243:8001"

def fetch_and_print(endpoint, description):
    """Fetches data from an API endpoint and prints it."""
    url = f"{API_BASE_URL}{endpoint}"
    print(f"\n--- Fetching: {description} from {url} ---")
    try:
        response = requests.get(url, timeout=10) # 10 second timeout
        response.raise_for_status()  # Raises an HTTPError for bad responses (4XX or 5XX)
        
        data = response.json()
        print(json.dumps(data, indent=2, ensure_ascii=False)) # Pretty print JSON
        return data
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print(f"Response content: {response.text}")
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error occurred: {conn_err}")
        print(f"Is the API server running at {API_BASE_URL}?")
    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout error occurred: {timeout_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"An error occurred: {req_err}")
    except json.JSONDecodeError:
        print("Failed to decode JSON from response.")
        print(f"Response content: {response.text}")
    return None

def main():
    print(f"Attempting to validate API at {API_BASE_URL}")

    # 1. Validate /docs endpoint (checks if API is up and FastAPI docs are served)
    # We expect HTML, so we'll just check status code
    docs_url = f"{API_BASE_URL}/docs"
    print(f"\n--- Checking: API documentation at {docs_url} ---")
    try:
        response = requests.get(docs_url, timeout=5)
        if response.status_code == 200:
            print(f"SUCCESS: /docs endpoint is reachable (Status Code: {response.status_code})")
        else:
            print(f"WARNING: /docs endpoint returned status {response.status_code}")
    except Exception as e:
        print(f"ERROR: Could not reach /docs: {e}")


    # 2. Validate /dashboard_data endpoint
    dashboard_data = fetch_and_print("/dashboard_data", "Main Dashboard Data")

    # 3. Validate /station_detail/{station_id} endpoint (if dashboard_data was successful)
    if dashboard_data and isinstance(dashboard_data, list) and len(dashboard_data) > 0:
        # Try to get details for the first station from the dashboard_data
        first_station_id = dashboard_data[0].get('station_id')
        if first_station_id:
            fetch_and_print(f"/station_detail/{first_station_id}", f"Station Detail for ID: {first_station_id}")
        else:
            print("\nWARNING: Could not find a station_id in /dashboard_data to test /station_detail.")
    elif dashboard_data is None:
        print("\nSkipping /station_detail test because /dashboard_data fetch failed or returned no data.")
    else:
         print(f"\nSkipping /station_detail test. /dashboard_data format was unexpected or empty: {type(dashboard_data)}")


    # 4. Validate /ebike_heatmap_data endpoint
    fetch_and_print("/ebike_heatmap_data", "E-bike Heatmap Data")
    
    # 5. Validate /reallocation_matrix_data endpoint (expected to be not implemented yet)
    fetch_and_print("/reallocation_matrix_data", "Reallocation Matrix Data")


if __name__ == "__main__":
    main()