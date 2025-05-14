# Konsument Kafka -> MongoDB
# Subskrybuje trzy topic-i: station_information, station_status, vehicle_types

import json
from kafka import KafkaConsumer
from pymongo import MongoClient
from datetime import datetime


BOOTSTRAP_SERVERS = ['broker:9092']
TOPICS = ['information_station', 'station_status', 'vehicle_types']

MONGO_URI = 'mongodb://root:admin@mongo:27017/'
client = MongoClient(MONGO_URI, authSource='admin')
db = client['rta_project']


COLLECTIONS = {
    'information_station': db['station_information'],
    'station_status':      db['station_status'],
    'vehicle_types':       db['vehicle_types'],
}


consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='latest',
    value_deserializer=lambda b: json.loads(b.decode('utf-8'))
)

print("🛰️  Konsument uruchomiony, wczytuję dane i zapisuję do MongoDB...")

for msg in consumer:
    topic = msg.topic
    record = msg.value
    feed_type = record.get('feed_type', topic)
    data = record.get('data', {})
    collected_at = record.get('collected_at')

    docs = []
    if topic in ['station_status', 'information_station']:
        # Struktura: data['data']['stations']
        stations = data.get('data', {}).get('stations', [])
        for s in stations:
            docs.append({
                'feed':        topic,
                'collected_at': collected_at,
                **s
            })
    elif topic == 'vehicle_types':
        vehicles = data.get('data', {}).get('vehicle_types', [])
        for v in vehicles:
            docs.append({
                'feed':        topic,
                'collected_at': collected_at,
                **v
            })
    else:
        docs.append({
            'feed':  topic,
            'payload': record,
            'collected_at': collected_at
        })

    if docs:
        coll = COLLECTIONS.get(topic)
        try:
            coll.insert_many(docs)
            print(f"✔️  Wstawiono {len(docs)} dokumentów do kolekcji '{topic}'")
        except Exception as e:
            print(f"❌ Błąd zapisu do MongoDB: {e}")

