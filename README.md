# README.md

## Uruchomienie

1. W katalogu projektu:

   ```bash
   docker-compose up -d
   ```

2. Otworz JupyterLab: [http://localhost:8889](http://localhost:8889) (token: `root`)

## Kafka topics

W terminalu JupyterLab:

```bash
cd ~/notebooks
bash create_topics.sh 
```

## Producer

W terminalu JupyterLab:

```bash
cd ~/notebooks
python3 producer.py
```

Publikuje dane do Kafka topic

- `station_information` - co 1h
- `station_status` - co 1m
- `vehicle_types` - co 1d
