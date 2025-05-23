import streamlit as st
import pandas as pd
import requests
from streamlit_folium import st_folium
import folium
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(layout="wide")
st.title("🚲 Dashboard Rowerowy – Dane na żywo")

if st.button("🔄 Odśwież dane"):
    st.rerun()

# --- Dane z API
@st.cache_data(ttl=30)
def load_data():
    r = requests.get("http://localhost:8000/dashboard_data")
    return pd.DataFrame(r.json())

df = load_data()

# --- Przetwarzanie
df["actual/capacity"] = (df["num_classic_bikes"] + df["num_ebikes"]).astype(str) + " / " + df["capacity"].astype(str)
df["% future"] = (df["forecast_3h_occupancy_ratio"] * 100).round(1).fillna(0)
df["type"] = df["num_ebikes"].apply(lambda x: "e" if x > 0 else "e-z")

# --- Tabela główna
st.subheader("📋 Tabela stacji")

# Lista unikalnych wartości + '(wszystkie)'
actions = ["(wszystkie)"] + sorted(df["action"].dropna().unique().tolist())
techs = ["(wszystkie)"] + sorted(df["tech_status"].dropna().unique().tolist())
types = ["(wszystkie)"] + sorted(df["type"].dropna().unique().tolist())

# Widgety filtrowania
selected_action = st.selectbox("Filtruj po Action:", actions)
selected_tech = st.selectbox("Filtruj po Tech Status:", techs)
selected_type = st.selectbox("Filtruj po Type:", types)

# Filtrowanie danych
filtered_df = df.copy()
if selected_action != "(wszystkie)":
    filtered_df = filtered_df[filtered_df["action"] == selected_action]
if selected_tech != "(wszystkie)":
    filtered_df = filtered_df[filtered_df["tech_status"] == selected_tech]
if selected_type != "(wszystkie)":
    filtered_df = filtered_df[filtered_df["type"] == selected_type]

st.dataframe(filtered_df[[
    "name", "actual/capacity", "forecast_3h_bikes", "% future",
    "action", "tech_status", "type"
]].rename(columns={
    "name": "Station",
    "actual/capacity": "Actual / Capacity",
    "forecast_3h_bikes": "Forecast 3h",
    "% future": "% Future",
    "action": "Action",
    "tech_status": "Tech Status",
    "type": "Type"
}), use_container_width=True)


# --- Mapa z markerami
st.subheader("🗺️ Mapa stacji")

def get_color(action):
    if "ZABIERZ" in action:
        return "red"
    elif "DOSTARCZ" in action:
        return "green"
    elif "SERWIS" in action:
        return "orange"
    else:
        return "blue"

lat_center = df["latitude"].mean()
lon_center = df["longitude"].mean()
m = folium.Map(location=[lat_center, lon_center], zoom_start=13)

for _, row in df.iterrows():
    if pd.notna(row["latitude"]) and pd.notna(row["longitude"]):
        num_bikes = row["num_classic_bikes"] + row["num_ebikes"]
        folium.CircleMarker(
            location=[row["latitude"], row["longitude"]],
            radius=6,
            color=get_color(row["action"]),
            fill=True,
            fill_opacity=0.8,
            tooltip=f"{row['name']}<br>{row['action']}<br>{num_bikes} bikes"
        ).add_to(m)

_ = st_folium(m, width=1000, height=500)

# --- Szczegóły stacji

st.subheader("🔍 Szczegóły stacji")

selected = st.selectbox("Wybierz stację:", df["name"].sort_values().unique())

@st.cache_data(ttl=30)
def fetch_station_details(station_id):
    url = f"http://localhost:8000/station_detail/{station_id}"
    return requests.get(url).json()

station_row = df[df["name"] == selected]
if not station_row.empty:
    station_id = station_row.iloc[0]["station_id"]
    detail = fetch_station_details(station_id)

    if detail and "pie_chart_data" in detail and "time_series" in detail:
        pie = go.Figure(go.Pie(
            labels=["E-bikes", "Classic", "Empty docks"],
            values=[
                detail["pie_chart_data"].get("ebikes", 0),
                detail["pie_chart_data"].get("classic_bikes", 0),
                detail["pie_chart_data"].get("empty_docks", 0)
            ]
        ))
        pie.update_layout(title="Zajętość doków")
        st.plotly_chart(pie, use_container_width=True)

        ts_df = pd.DataFrame(detail["time_series"])
        fig = px.line(ts_df, x="timestamp", y="value", color="type",
                      title="Rowerów w czasie (rzeczywiste + prognoza)")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("⚠️ Brak danych szczegółowych dla wybranej stacji.")
else:
    st.error("❌ Nie udało się pobrać ID stacji.")


#Heatmapa e-rowerów
st.subheader("🔥 Heatmapa e-rowerów")

# Filtrowanie: tylko stacje z poprawnymi współrzędnymi
heat_df = df.dropna(subset=["latitude", "longitude", "num_ebikes"]).copy()
heat_df = df[df["num_ebikes"] > 0]
# Środek mapy
lat_center = heat_df["latitude"].mean()
lon_center = heat_df["longitude"].mean()

# Mapa + Heatmapa
from folium.plugins import HeatMap
heatmap_map = folium.Map(location=[lat_center, lon_center], zoom_start=13)

# Przygotowanie danych: [lat, lon, waga]
heat_data = heat_df[["latitude", "longitude", "num_ebikes"]].values.tolist()
HeatMap(heat_data, radius=15).add_to(heatmap_map)

# Wyświetlenie w Streamlit
_ = st_folium(heatmap_map, width=1000, height=500)


#Mapa relokacji

st.subheader("🚚 Relokacje rowerów")

df["action"] = df["action"].fillna("")  # Na wszelki wypadek

# Filtrowanie
zabierz_df = df[df["action"].str.upper().str.contains("ZABIERZ")]
dostarcz_df = df[df["action"].str.upper().str.contains("DOSTARCZ")]

if not zabierz_df.empty and not dostarcz_df.empty:
    st.markdown(f"🔴 ZABIERZ: {len(zabierz_df)} stacji")
    st.markdown(f"🟢 DOSTARCZ: {len(dostarcz_df)} stacji")

    relokacje = list(zip(
        zabierz_df.head(5).itertuples(index=False),
        dostarcz_df.head(5).itertuples(index=False)
    ))

    lat_center = df["latitude"].mean()
    lon_center = df["longitude"].mean()
    reloc_map = folium.Map(location=[lat_center, lon_center], zoom_start=13)

    for from_station, to_station in relokacje:
        if pd.notna(from_station.latitude) and pd.notna(to_station.latitude):
            folium.Marker(
                location=[from_station.latitude, from_station.longitude],
                tooltip=f"ZABIERZ: {from_station.name}",
                icon=folium.Icon(color="red", icon="minus")
            ).add_to(reloc_map)

            folium.Marker(
                location=[to_station.latitude, to_station.longitude],
                tooltip=f"DOSTARCZ: {to_station.name}",
                icon=folium.Icon(color="green", icon="plus")
            ).add_to(reloc_map)

            folium.PolyLine(
                locations=[
                    [from_station.latitude, from_station.longitude],
                    [to_station.latitude, to_station.longitude]
                ],
                color="blue",
                weight=3,
                opacity=0.8,
                tooltip=f"{from_station.name} → {to_station.name}"
            ).add_to(reloc_map)

    with st.spinner("📦 Generuję mapę relokacji..."):
        st_folium(reloc_map, width=1000, height=500)

else:
    st.info("ℹ️ Brak stacji do relokacji – nic do ZABRANIA.")