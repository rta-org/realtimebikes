# dashboard_api.py
from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
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
            sort=[('decision_timestamp', MongoClient.DESCENDING)]
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
        sort=[('decision_timestamp', MongoClient.DESCENDING)]
    )
    forecast_data = db[FORECAST_COLLECTION].find_one(
        {'station_id': station_id},
        sort=[('forecast_generated_at', MongoClient.DESCENDING)]
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


@app.get("/reallocation_matrix_data")
async def get_reallocation_matrix_data():
    # TODO: Implementacja logiki macierzy relokacji
    # Wymagałoby to algorytmu, który na podstawie akcji DOSTARCZ/ZABIERZ
    # generuje konkretne przepływy rowerów między stacjami.
    # Na razie zwracamy placeholder.
    return {"message": "Endpoint /reallocation_matrix_data - Not Implemented Yet"}

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
# uvicorn dashboard_api:app --reload