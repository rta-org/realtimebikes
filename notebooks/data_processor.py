# data_processor.py
import pymongo
from kafka import KafkaProducer
import json
import time
from datetime import datetime, timezone
import logging
import math

# Konfiguracja logowania
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Konfiguracja MongoDB
MONGO_URI = 'mongodb://root:admin@mongo:27017/' # Użyj localhost jeśli uruchamiasz poza Dockerem
MONGO_DATABASE = 'rta_project'
STATION_INFO_COLLECTION = 'station_information'
STATION_STATUS_COLLECTION = 'station_status'
VEHICLE_TYPES_COLLECTION = 'vehicle_types'
PROCESSED_DATA_COLLECTION = 'processed_station_data' # Nowa kolekcja

# Konfiguracja Kafki (opcjonalne wyjście)
KAFKA_BOOTSTRAP_SERVERS = ['broker:9092'] # Użyj localhost:29092 jeśli uruchamiasz poza Dockerem
PROCESSED_DATA_TOPIC = 'processed_station_data_topic'

VEHICLE_TYPE_ID_ELECTRIC = '2' # Zgodnie z opisem projektu

# Globalne cache dla danych rzadko zmieniających się
station_info_cache = {}
vehicle_types_cache = {}

def get_mongo_client():
    return pymongo.MongoClient(MONGO_URI, authSource='admin')

def load_static_data(db):
    """Ładuje dane stacji i typów pojazdów do pamięci podręcznej."""
    global station_info_cache, vehicle_types_cache
    
    station_info_cursor = db[STATION_INFO_COLLECTION].find({})
    for info in station_info_cursor:
        station_info_cache[info['station_id']] = info
    logging.info(f"Załadowano {len(station_info_cache)} rekordów informacji o stacjach.")

    vehicle_types_cursor = db[VEHICLE_TYPES_COLLECTION].find({})
    for vt in vehicle_types_cursor:
        vehicle_types_cache[vt['vehicle_type_id']] = vt
    logging.info(f"Załadowano {len(vehicle_types_cache)} typów pojazdów.")
    # Jeśli vehicle_types jest puste, można dodać domyślne wartości
    if not vehicle_types_cache:
        logging.warning("Brak danych o typach pojazdów w DB. Używam domyślnych mapowań.")
        # To jest uproszczenie; idealnie vehicle_types.json powinno być źródłem prawdy
        vehicle_types_cache['1'] = {"vehicle_type_id": "1", "name": "classic"}
        vehicle_types_cache[VEHICLE_TYPE_ID_ELECTRIC] = {"vehicle_type_id": VEHICLE_TYPE_ID_ELECTRIC, "name": "electric"}


def process_station_status(status_doc, db_client):
    db = db_client[MONGO_DATABASE]
    station_id = status_doc.get('station_id')
    info = station_info_cache.get(station_id)

    if not info:
        logging.warning(f"Brak informacji dla stacji {station_id}. Pomijam.")
        return None

    capacity = info.get('capacity', 0)
    num_vehicles_available = status_doc.get('num_vehicles_available', 0)
    
    # Obliczanie typów rowerów
    num_ebikes_available = 0
    num_classic_bikes_available = 0
    if 'vehicle_types_available' in status_doc:
        for vehicle_type_info in status_doc['vehicle_types_available']:
            type_id = vehicle_type_info.get('vehicle_type_id')
            count = vehicle_type_info.get('count', 0)
            if type_id == VEHICLE_TYPE_ID_ELECTRIC:
                num_ebikes_available += count
            else: # Zakładamy, że wszystko inne to klasyczne lub nieznane
                num_classic_bikes_available += count
    
    # Identyfikacja aktywnych stacji
    is_renting = status_doc.get('is_renting', False)
    tech_status = "OK" if is_renting else "SERWIS NEEDED"

    # Obliczanie Actual / Capacity
    actual_vs_capacity_ratio = 0
    if capacity > 0:
        actual_vs_capacity_ratio = num_vehicles_available / capacity
    
    # Przygotowanie danych dla modelu prognozującego
    # `delta_num_docks_available` wymaga stanu poprzedniego, upraszczamy na razie
    # Można by to zrealizować poprzez odpytanie poprzedniego rekordu dla tej stacji
    # lub użycie narzędzia do przetwarzania strumieniowego
    percent_of_vehicles_available = actual_vs_capacity_ratio * 100 # lub (num_vehicles_available / capacity) * 100

    processed_at = datetime.now(timezone.utc)

    processed_data = {
        'station_id': station_id,
        'name': info.get('name'),
        'latitude': info.get('lat'),
        'longitude': info.get('lon'),
        'capacity': capacity,
        'num_vehicles_available': num_vehicles_available,
        'num_docks_available': status_doc.get('num_docks_available', 0),
        'num_ebikes_available': num_ebikes_available,
        'num_classic_bikes_available': num_classic_bikes_available,
        'is_renting': is_renting,
        'tech_status': tech_status,
        'actual_vs_capacity_ratio': actual_vs_capacity_ratio,
        'percent_of_vehicles_available': percent_of_vehicles_available, # Cecha dla prognozowania
        'last_reported': status_doc.get('last_reported'), # Z oryginalnego statusu
        'collected_at': status_doc.get('collected_at'), # Z oryginalnego statusu od konsumenta
        'processed_at': processed_at.isoformat()
    }
    return processed_data

def main():
    mongo_client = get_mongo_client()
    db = mongo_client[MONGO_DATABASE]
    
    load_static_data(db) # Załaduj raz na starcie

    # Opcjonalny Kafka producer
    # producer = KafkaProducer(
    #     bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    #     value_serializer=lambda v: json.dumps(v).encode('utf-8')
    # )

    # Przykładowa pętla przetwarzania co X sekund
    # W bardziej zaawansowanym scenariuszu można użyć change streams MongoDB
    # lub konsumować bezpośrednio z tematu Kafki `station_status_topic`
    while True:
        logging.info("Rozpoczynam cykl przetwarzania danych stacji...")
        all_status_docs = list(db[STATION_STATUS_COLLECTION].find({})) # Można optymalizować, np. tylko najnowsze
        
        processed_batch = []
        for status_doc in all_status_docs:
            # Symulacja przetwarzania tylko najnowszych wpisów per stacja
            # W praktyce, to by wymagało bardziej złożonej logiki agregacji
            # lub obsługi change streamów. Dla przykładu, przetwarzamy wszystko co znajdziemy.
            
            processed_data = process_station_status(status_doc, mongo_client)
            if processed_data:
                processed_batch.append(processed_data)
        
        if processed_batch:
            try:
                # Zapis do nowej kolekcji MongoDB
                # Używamy replace_one z upsert=True aby aktualizować istniejące lub wstawiać nowe
                # na podstawie station_id. Można też po prostu wstawiać jako nowe dokumenty z timestampem.
                # Dla uproszczenia, tutaj zastępujemy cały dokument.
                for data in processed_batch:
                    db[PROCESSED_DATA_COLLECTION].replace_one(
                        {'station_id': data['station_id']},
                        data,
                        upsert=True
                    )
                logging.info(f"Zapisano/zaktualizowano {len(processed_batch)} przetworzonych dokumentów w MongoDB.")

                # Opcjonalny zapis do Kafki
                # for data in processed_batch:
                #     producer.send(PROCESSED_DATA_TOPIC, data)
                # producer.flush()
                # logging.info(f"Wysłano {len(processed_batch)} przetworzonych dokumentów do Kafki.")

            except Exception as e:
                logging.error(f"Błąd podczas zapisu przetworzonych danych: {e}")
        else:
            logging.info("Brak nowych danych statusu do przetworzenia.")
            
        time.sleep(60) # Uruchom co 60 sekund

if __name__ == "__main__":
    main()