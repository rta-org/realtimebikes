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

 *Pozwól mu działać przez co najmniej minutę po starcie, aby wysłał początkowe dane.*

2.  **Konsument Danych (Data Consumer - `consumer.py`):**
    Subskrybuje tematy Kafka i zapisuje dane do MongoDB.
    ```bash
    python3 consumer.py
    ```
    *Pozwól mu działać, aby przetworzył przynajmniej początkowe dane wysłane przez producenta (szczególnie `information_station`).*

3.  **Przetwarzanie Danych (Data Processor - `data_processor.py`):**
    Przetwarza surowe dane z MongoDB, wzbogaca je i zapisuje do kolekcji `processed_station_data`.
    ```bash
    python3 data_processor.py
    ```

4.  **Serwis Prognozowania (Forecasting Service - `forecasting_service.py`):**
    Generuje prognozy zapotrzebowania na rowery i zapisuje je do MongoDB.
    ```bash
    python3 forecasting_service.py
    ```

5.  **Serwis Decyzyjny (Decision Service - `decision_service.py`):**
    Podejmuje decyzje o relokacji na podstawie przetworzonych danych i prognoz, zapisując akcje do MongoDB.
    ```bash
    python3 decision_service.py
    ```

6.  **API Dashboardu (Dashboard API - `dashboard_api.py`):**
    Uruchamia serwer API (FastAPI) udostępniający przetworzone dane. Uruchamiane za pomocą Uvicorn.
    ```bash
    uvicorn dashboard_api:app --host 0.0.0.0 --port 8000 --reload
    ```
    * API będzie dostępne wewnątrz sieci Docker pod adresem `http://localhost:8000`.
    * Z zewnątrz (np. z Twojej przeglądarki lub Streamlit dashboardu) będzie dostępne przez port zmapowany w `compose.yaml` (`8001`):
        * Dokumentacja API (Swagger UI): `http://<ip-remote-host>:8001/docs`albo `http://localhost:8001/docs`

     
7. **Wizualizacja (dashboard - `dashboard.py`):**
   Wizualizuje przetworzone dane.
   ```bash
    streamlit run dashboard.py
   ```
