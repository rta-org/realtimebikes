# dashboard_api.py
from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
import pymongo
from pydantic import BaseModel # Do walidacji typów, opcjonalne
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

MONGO_URI = 'mongodb://root:admin@mongo:27017/'
MONGO_DATABASE = 'rta_project'
PROCESSED_DATA_COLLECTION = 'processed_station_data'
FORECAST_COLLECTION = 'station_forecasts'
ACTION_COLLECTION = 'station_actions'
STATION_INFO_COLLECTION = 'station_information' # Może być potrzebne dla dodatkowych info

app = FastAPI(title="Bike Share Dashboard API")
mongo_client = None

@app.on_event("startup")
async def startup_db_client():
    global mongo_client
    mongo_client = MongoClient(MONGO_URI, authSource='admin')
    logging.info("Nawiązano połączenie z MongoDB.")

@app.on_event("shutdown")
async def shutdown_db_client():
    if mongo_client:
        mongo_client.close()
        logging.info("Zamknięto połączenie z MongoDB.")

def get_db():
    if mongo_client is None:
        # To się nie powinno zdarzyć po poprawnym starcie, ale jako fallback
        startup_db_client() 
    return mongo_client[MONGO_DATABASE]

# --- Pydantic Models (opcjonalne, ale dobre dla walidacji i dokumentacji API) ---
class StationDashboardData(BaseModel):
    station_id: str
    name: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    actual_num_bikes: int
    capacity: int
    actual_vs_capacity_ratio: float # Aktualne obłożenie (num_vehicles_available / capacity)
    num_ebikes: int
    num_classic_bikes: int
    forecast_3h_bikes: Optional[float] # Przewidywana liczba rowerów za 3h (future_bikes)
    forecast_3h_occupancy_ratio: Optional[float] # Przewidywane obłożenie za 3h (ratio z modelu dec.)
    action: str
    tech_status: str
    last_reported: Optional[str]

class PieChartData(BaseModel):
    ebikes: int
    classic_bikes: int
    empty_docks: int

class TimeSeriesPoint(BaseModel):
    timestamp: str
    value: int
    type: str # 'actual' or 'forecast'

class StationDetailData(BaseModel):
    station_id: str
    name: Optional[str]
    capacity: int
    pie_chart_data: PieChartData
    time_series: List[TimeSeriesPoint]
    # ... inne potrzebne informacje
    current_action: str
    tech_status: str


# --- API Endpoints ---

@app.get("/dashboard_data", response_model=List[StationDashboardData])
async def get_dashboard_data():
    db = get_db()
    dashboard_items = []

    # Pobieramy wszystkie najnowsze akcje (które powinny zawierać dane z poprzednich kroków)
    # Alternatywnie, łączymy dane z processed_data i actions
    
    # Podejście 1: Zaczynamy od kolekcji 'actions', zakładając, że jest aktualna
    # i zawiera kluczowe informacje lub odnośniki.
    # Lepsze podejście: zacznij od processed_station_data i dołącz prognozy i akcje.

    processed_stations = list(db[PROCESSED_DATA_COLLECTION].find({}))
    
    for station_proc_data in processed_stations:
        station_id = station_proc_data['station_id']
        
        # Pobierz najnowszą akcję
        action_data = db[ACTION_COLLECTION].find_one(
            {'station_id': station_id},
            sort=[('decision_timestamp', pymongo.DESCENDING)]
        )
        
        # Pobierz najnowszą prognozę (nie jest bezpośrednio potrzebna tutaj,
        # bo action_data już powinno zawierać future_bikes i future_occupancy_ratio)
        # forecast_data = db[FORECAST_COLLECTION].find_one(...) 

        item = StationDashboardData(
            station_id=station_id,
            name=station_proc_data.get('name'),
            latitude=station_proc_data.get('latitude'),
            longitude=station_proc_data.get('longitude'),
            actual_num_bikes=station_proc_data.get('num_vehicles_available', 0),
            capacity=station_proc_data.get('capacity', 0),
            actual_vs_capacity_ratio=station_proc_data.get('actual_vs_capacity_ratio', 0.0),
            num_ebikes=station_proc_data.get('num_ebikes_available', 0),
            num_classic_bikes=station_proc_data.get('num_classic_bikes_available', 0),
            forecast_3h_bikes=action_data.get('future_bikes_3h') if action_data else None,
            forecast_3h_occupancy_ratio=action_data.get('future_occupancy_ratio_3h') if action_data else None,
            action=action_data.get('action', 'BRAK DANYCH') if action_data else 'BRAK DANYCH',
            tech_status=station_proc_data.get('tech_status', 'BRAK DANYCH'),
            last_reported=station_proc_data.get('last_reported')
        )
        dashboard_items.append(item)
        
    return dashboard_items

@app.get("/station_detail/{station_id}", response_model=Optional[StationDetailData])
async def get_station_detail(station_id: str):
    db = get_db()
    
    proc_data = db[PROCESSED_DATA_COLLECTION].find_one({'station_id': station_id})
    if not proc_data:
        raise HTTPException(status_code=404, detail="Station processed data not found")

    action_data = db[ACTION_COLLECTION].find_one(
        {'station_id': station_id},
        sort=[('decision_timestamp', pymongo.DESCENDING)]
    )
    forecast_data = db[FORECAST_COLLECTION].find_one(
        {'station_id': station_id},
        sort=[('forecast_generated_at', pymongo.DESCENDING)]
    )

    capacity = proc_data.get('capacity', 0)
    num_ebikes = proc_data.get('num_ebikes_available', 0)
    num_classic = proc_data.get('num_classic_bikes_available', 0)
    num_total_bikes = proc_data.get('num_vehicles_available', 0)
    empty_docks = capacity - num_total_bikes # lub proc_data.get('num_docks_available', 0)

    pie_data = PieChartData(
        ebikes=num_ebikes,
        classic_bikes=num_classic, # num_total_bikes - num_ebikes
        empty_docks=max(0, empty_docks)
    )

    time_series: List[TimeSeriesPoint] = []
    # 1. Aktualne dane (jako jeden punkt 'actual')
    # Można by pobrać historię z `station_status`, jeśli była by skonsumowana w odpowiedni sposób
    # Dla uproszczenia, weźmy ostatni `collected_at` lub `processed_at`
    actual_ts = proc_data.get('collected_at') or proc_data.get('processed_at')
    if actual_ts:
         time_series.append(TimeSeriesPoint(
            timestamp=actual_ts,
            value=num_total_bikes,
            type='actual'
        ))

    # 2. Dane prognozy (linia przerywana)
    if forecast_data and 'hourly_forecasts' in forecast_data:
        current_bikes = num_total_bikes
        for hf in forecast_data['hourly_forecasts']:
            # Obliczamy stan rowerów na koniec każdej godziny prognozy
            current_bikes = current_bikes - hf['forecast_out'] + hf['forecast_in']
            current_bikes = max(0, min(current_bikes, capacity)) # Ogranicz do [0, capacity]
            time_series.append(TimeSeriesPoint(
                timestamp=hf['forecast_timestamp'],
                value=current_bikes,
                type='forecast'
            ))
    
    # Sortuj time_series po timestampie
    time_series.sort(key=lambda x: x.timestamp)

    return StationDetailData(
        station_id=station_id,
        name=proc_data.get('name'),
        capacity=capacity,
        pie_chart_data=pie_data,
        time_series=time_series,
        current_action=action_data.get('action', 'BRAK DANYCH') if action_data else 'BRAK DANYCH',
        tech_status=proc_data.get('tech_status', 'BRAK DANYCH')
    )

from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
import pymongo # For pymongo.DESCENDING
from pydantic import BaseModel
from typing import List, Optional, Dict, Any # Ensure Dict and Any are imported
from datetime import datetime, timedelta
import logging # Ensure logging is imported

# Pydantic model for the reallocation output
class ReallocationMove(BaseModel):
    from_station_id: str
    from_station_name: Optional[str] = "Unknown Station"
    to_station_id: str
    to_station_name: Optional[str] = "Unknown Station"
    bikes_to_move: int


@app.get("/reallocation_matrix_data", response_model=List[ReallocationMove])
async def get_reallocation_matrix_data():
    db = get_db()
    try:
        all_actions_cursor = db[ACTION_COLLECTION].find({})
        all_actions = list(all_actions_cursor) # Convert cursor to list
    except Exception as e:
        logging.error(f"Error fetching from ACTION_COLLECTION: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch station actions from database.")

    sources = []  # Stations that can give bikes
    sinks = []    # Stations that need bikes

    if not all_actions:
        logging.info("No actions found in ACTION_COLLECTION for reallocation.")
        return []

    for action_doc in all_actions:
        num_to_move = action_doc.get('num_to_move', 0)
        station_id = action_doc.get('station_id')
        
        if not station_id: # Skip if essential data is missing
            logging.warning(f"Action document missing station_id, skipping: {action_doc.get('_id')}")
            continue
            
        station_name = action_doc.get('name', f"Station ID {station_id}") # Use station_id if name is missing

        if num_to_move < 0:  # Station has a surplus (ZABIERZ action means num_to_move is negative)
            sources.append({
                'station_id': station_id,
                'name': station_name,
                'available_bikes': -num_to_move  # Make it a positive supply count
            })
        elif num_to_move > 0:  # Station has a deficit (DOSTARCZ action means num_to_move is positive)
            sinks.append({
                'station_id': station_id,
                'name': station_name,
                'needed_bikes': num_to_move
            })

    reallocations: List[ReallocationMove] = []

    if not sources or not sinks:
        logging.info(f"Reallocation: Not enough sources ({len(sources)}) or sinks ({len(sinks)}) to perform moves.")
        return []
        
    logging.info(f"Reallocation: Starting with {len(sources)} sources and {len(sinks)} sinks.")

    # Simple greedy matching algorithm
    source_idx = 0
    sink_idx = 0

    while source_idx < len(sources) and sink_idx < len(sinks):
        source = sources[source_idx]
        sink = sinks[sink_idx]

        if source['available_bikes'] == 0: # Current source is exhausted
            source_idx += 1
            continue
        
        if sink['needed_bikes'] == 0: # Current sink is satisfied
            sink_idx += 1
            continue

        # Determine how many bikes can be moved in this step
        bikes_to_transfer = min(source['available_bikes'], sink['needed_bikes'])

        if bikes_to_transfer > 0:
            move = ReallocationMove(
                from_station_id=source['station_id'],
                from_station_name=source['name'],
                to_station_id=sink['station_id'],
                to_station_name=sink['name'],
                bikes_to_move=bikes_to_transfer
            )
            reallocations.append(move)
            logging.info(f"Reallocation move: {bikes_to_transfer} bikes from {source['name']} to {sink['name']}")

            source['available_bikes'] -= bikes_to_transfer
            sink['needed_bikes'] -= bikes_to_transfer
        
        # Move to the next source if the current one is exhausted
        if source['available_bikes'] == 0:
            source_idx += 1
        
        # Move to the next sink if the current one is satisfied
        if sink['needed_bikes'] == 0:
            sink_idx += 1
            
    if not reallocations:
        logging.info("Reallocation: No actual moves generated despite having sources/sinks (e.g., all needs met or no supply).")
    else:
        logging.info(f"Reallocation: Generated {len(reallocations)} moves.")
        
    return reallocations

# --- Heatmap Data Endpoint ---
# Endpoint do pobierania danych do heatmapy
# Można dodać filtr, np. tylko aktywne stacje
@app.get("/ebike_heatmap_data")
async def get_ebike_heatmap_data():
    db = get_db()
    heatmap_data = []
    # Pobierz wszystkie stacje z przetworzonych danych
    stations_cursor = db[PROCESSED_DATA_COLLECTION].find(
        {}, # Można dodać filtr, np. tylko aktywne stacje
        {"latitude": 1, "longitude": 1, "num_ebikes_available": 1, "_id": 0}
    )
    for station in stations_cursor:
        if station.get("latitude") and station.get("longitude"):
            heatmap_data.append({
                "lat": station["latitude"],
                "lng": station["longitude"],
                "weight": station.get("num_ebikes_available", 0) # Waga to liczba e-rowerów
            })
    return heatmap_data

# Aby uruchomić API (z głównego katalogu projektu, gdzie jest ten plik):
# uvicorn dashboard_api:app --host 0.0.0.0 --port 8000 --reload