# forecasting_service.py
import pymongo
from kafka import KafkaProducer
import json
import time
from datetime import datetime, timedelta, timezone
import logging
import random # Dla prognoz-placeholderów

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

MONGO_URI = 'mongodb://root:admin@mongo:27017/'
MONGO_DATABASE = 'rta_project'
PROCESSED_DATA_COLLECTION = 'processed_station_data'
FORECAST_COLLECTION = 'station_forecasts' # Nowa kolekcja

KAFKA_BOOTSTRAP_SERVERS = ['broker:9092']
FORECAST_TOPIC = 'station_forecasts_topic'

def get_mongo_client():
    return pymongo.MongoClient(MONGO_URI, authSource='admin')

def generate_dummy_forecasts(station_data):
    """
    Generuje prognozy-placeholdery.
    W rzeczywistości tutaj byłby zaawansowany model ML/statystyczny.
    """
    # Prognoza na 3 godziny, co godzinę
    forecasts = []
    base_demand = station_data.get('capacity', 10) * 0.1 # 10% pojemności jako bazowe zapotrzebowanie

    # Prosta heurystyka na podstawie pory dnia (placeholder)
    current_hour = datetime.now(timezone.utc).hour
    if 6 <= current_hour < 10: # Poranny szczyt
        demand_multiplier = 1.5
    elif 16 <= current_hour < 19: # Popołudniowy szczyt
        demand_multiplier = 1.8
    else:
        demand_multiplier = 0.8
        
    for i in range(3): # Horyzont 3h
        forecast_time = datetime.now(timezone.utc) + timedelta(hours=i+1)
        # Uproszczona prognoza, w rzeczywistości wartości mogą być różne
        forecast_out = max(0, int(random.uniform(0.5, 1.5) * base_demand * demand_multiplier))
        forecast_in = max(0, int(random.uniform(0.5, 1.5) * base_demand * demand_multiplier * 0.8)) # Mniej zwrotów
        
        forecasts.append({
            'forecast_timestamp': forecast_time.isoformat(),
            'forecast_out': forecast_out, # przewidywana liczba wypożyczeń
            'forecast_in': forecast_in    # przewidywana liczba zwrotów
        })
    return forecasts

def main():
    mongo_client = get_mongo_client()
    db = mongo_client[MONGO_DATABASE]

    # producer = KafkaProducer(
    #     bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    #     value_serializer=lambda v: json.dumps(v).encode('utf-8')
    # )

    while True:
        logging.info("Rozpoczynam cykl generowania prognoz...")
        # Pobierz wszystkie aktywne stacje z przetworzonych danych
        active_stations_cursor = db[PROCESSED_DATA_COLLECTION].find({'is_renting': True})
        
        forecast_batch_for_db = []

        for station_data in active_stations_cursor:
            station_id = station_data['station_id']
            hourly_forecasts = generate_dummy_forecasts(station_data)
            
            # Agregacja prognoz na 3h
            total_forecast_out_3h = sum(f['forecast_out'] for f in hourly_forecasts)
            total_forecast_in_3h = sum(f['forecast_in'] for f in hourly_forecasts)

            forecast_doc = {
                'station_id': station_id,
                'forecast_generated_at': datetime.now(timezone.utc).isoformat(),
                'forecast_horizon_hours': 3,
                'total_forecast_out_3h': total_forecast_out_3h,
                'total_forecast_in_3h': total_forecast_in_3h,
                'hourly_forecasts': hourly_forecasts # Szczegółowe prognozy godzinowe
            }
            forecast_batch_for_db.append(forecast_doc)

        if forecast_batch_for_db:
            try:
                for forecast_data in forecast_batch_for_db:
                     db[FORECAST_COLLECTION].replace_one(
                        {'station_id': forecast_data['station_id']},
                        forecast_data,
                        upsert=True
                    )
                logging.info(f"Zapisano/zaktualizowano {len(forecast_batch_for_db)} prognoz w MongoDB.")

                # for data in forecast_batch_for_db:
                #     producer.send(FORECAST_TOPIC, data)
                # producer.flush()
                # logging.info(f"Wysłano {len(forecast_batch_for_db)} prognoz do Kafki.")
            except Exception as e:
                 logging.error(f"Błąd podczas zapisu prognoz: {e}")
        else:
            logging.info("Brak aktywnych stacji do prognozowania lub błąd pobierania danych.")

        # Odświeżanie co 15 minut
        time.sleep(15 * 60) 

if __name__ == "__main__":
    main()