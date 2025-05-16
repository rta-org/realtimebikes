# decision_service.py
import pymongo
from kafka import KafkaProducer
import json
import time
from datetime import datetime, timezone
import logging
import math

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

MONGO_URI = 'mongodb://root:admin@mongo:27017/'
MONGO_DATABASE = 'rta_project'
PROCESSED_DATA_COLLECTION = 'processed_station_data'
FORECAST_COLLECTION = 'station_forecasts'
ACTION_COLLECTION = 'station_actions' # Nowa kolekcja

KAFKA_BOOTSTRAP_SERVERS = ['broker:9092']
ACTION_TOPIC = 'station_actions_topic'

THRESHOLD_MIN = 0.2  # 20% pojemności
THRESHOLD_MAX = 0.8  # 80% pojemności

def get_mongo_client():
    return pymongo.MongoClient(MONGO_URI, authSource='admin')

def determine_action(station_data, forecast_data):
    station_id = station_data['station_id']
    capacity = station_data.get('capacity', 0)
    curr_bikes = station_data.get('num_vehicles_available', 0)
    is_renting = station_data.get('is_renting', False)
    tech_status = station_data.get('tech_status', "OK") # Z data_processor

    action = "OK" # Domyślna akcja
    num_to_move = 0

    if not is_renting or tech_status == "SERWIS NEEDED":
        action = "SERWIS NEEDED"
        # Nie ma potrzeby obliczania `num_to_move` dla stacji w serwisie
    elif capacity == 0:
        action = "BŁĄD POJEMNOŚCI" # Stacja bez pojemności nie powinna być aktywna
    elif forecast_data:
        forecast_out_3h = forecast_data.get('total_forecast_out_3h', 0)
        forecast_in_3h = forecast_data.get('total_forecast_in_3h', 0)
        
        future_bikes = curr_bikes - forecast_out_3h + forecast_in_3h
        future_bikes = max(0, min(future_bikes, capacity)) # Ogranicz do [0, capacity]
        
        ratio = future_bikes / capacity if capacity > 0 else 0
        
        # Przechowujemy dla informacji w dashboardzie
        station_data['future_bikes_3h'] = future_bikes 
        station_data['future_occupancy_ratio_3h'] = ratio

        min_bikes_threshold = THRESHOLD_MIN * capacity
        max_bikes_threshold = THRESHOLD_MAX * capacity

        if ratio < THRESHOLD_MIN:
            needed = math.ceil(min_bikes_threshold - future_bikes)
            action = f"DOSTARCZ {int(needed)}"
            num_to_move = int(needed)
        elif ratio > THRESHOLD_MAX:
            remove = math.ceil(future_bikes - max_bikes_threshold)
            action = f"ZABIERZ {int(remove)}"
            num_to_move = -int(remove) # Ujemna wartość dla zabrania
        else:
            action = "OK"
    else:
        # Brak prognozy, nie można podjąć decyzji opartej na przyszłości
        # Można zaimplementować logikę opartą tylko na aktualnym stanie
        # lub oznaczyć jako "BRAK PROGNOZY"
        action = "BRAK PROGNOZY"
        logging.warning(f"Brak danych prognozy dla stacji {station_id}. Ustawiam akcję na BRAK PROGNOZY.")


    return {
        'station_id': station_id,
        'name': station_data.get('name'), # dla łatwiejszego debugowania
        'current_bikes': curr_bikes,
        'capacity': capacity,
        'future_bikes_3h': station_data.get('future_bikes_3h'),
        'future_occupancy_ratio_3h': station_data.get('future_occupancy_ratio_3h'),
        'action': action,
        'num_to_move': num_to_move, # Dodatnie dla dostarczenia, ujemne dla zabrania
        'tech_status': tech_status, # Przekazane z processed_data
        'decision_timestamp': datetime.now(timezone.utc).isoformat()
    }

def main():
    mongo_client = get_mongo_client()
    db = mongo_client[MONGO_DATABASE]

    # producer = KafkaProducer(
    #     bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    #     value_serializer=lambda v: json.dumps(v).encode('utf-8')
    # )

    # Model decyzyjny może działać z częstotliwością prognoz
    # lub częściej, jeśli chcemy reagować na zmiany w `processed_station_data`
    while True:
        logging.info("Rozpoczynam cykl podejmowania decyzji...")
        all_processed_stations = list(db[PROCESSED_DATA_COLLECTION].find({}))
        
        actions_batch_for_db = []

        for station_data in all_processed_stations:
            station_id = station_data['station_id']
            # Pobierz najnowszą prognozę dla stacji
            # Sortowanie po `forecast_generated_at` malejąco i wzięcie pierwszego
            latest_forecast = db[FORECAST_COLLECTION].find_one(
                {'station_id': station_id},
                sort=[('forecast_generated_at', pymongo.DESCENDING)]
            )
            
            action_doc = determine_action(station_data, latest_forecast)
            actions_batch_for_db.append(action_doc)

        if actions_batch_for_db:
            try:
                for action_data in actions_batch_for_db:
                    db[ACTION_COLLECTION].replace_one(
                        {'station_id': action_data['station_id']},
                        action_data,
                        upsert=True
                    )
                logging.info(f"Zapisano/zaktualizowano {len(actions_batch_for_db)} decyzji w MongoDB.")

                # for data in actions_batch_for_db:
                #     producer.send(ACTION_TOPIC, data)
                # producer.flush()
                # logging.info(f"Wysłano {len(actions_batch_for_db)} decyzji do Kafki.")
            except Exception as e:
                logging.error(f"Błąd podczas zapisu decyzji: {e}")
        else:
            logging.info("Brak stacji do przetworzenia dla modelu decyzyjnego.")
            
        # Uruchom co np. 5 minut (lub zsynchronizuj z odświeżaniem prognoz)
        time.sleep(5 * 60)

if __name__ == "__main__":
    main()