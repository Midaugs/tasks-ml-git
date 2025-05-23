import requests
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import pytz

def fetch_gyvenamosios_vietoves(url: str) -> pd.DataFrame:
    response = requests.get(url)
    if response.status_code == 200:
        json_data = response.json()
        records = json_data["_data"]

        # Extract relevant fields
        data_filtered = [
            {
                "gyv_kodas": r.get("gyv_kodas"),
                "tipas": r.get("tipas"),
                "tipo_santrumpa": r.get("tipo_santrumpa"),
                "pavadinimas_k": r.get("pavadinimas_k"),
                "pavadinimas": r.get("pavadinimas"),
            }
            for r in records
        ]
        return pd.DataFrame(data_filtered)
    else:
        raise ValueError(f"Failed to fetch data, status code: {response.status_code}")

def fetch_meteo_stations(url: str) -> pd.DataFrame:
    response = requests.get(url)
    if response.status_code == 200:
        stations = response.json()

        # Convert to DataFrame
        data = [
            {
                "code": s["code"],
                "name": s["name"],
                "latitude": s["coordinates"]["latitude"],
                "longitude": s["coordinates"]["longitude"]
            }
            for s in stations
        ]
        return pd.DataFrame(data)
    else:
        raise ValueError(f"Failed to fetch station data. Status code: {response.status_code}") 
    
def save_to_sqlite(df: pd.DataFrame, db_filename: str, table_name: str):
    with sqlite3.connect(db_filename) as conn:
        df.to_sql(table_name, conn, if_exists="replace", index=False)
    print(f"✅ Data saved to {db_filename} (table: {table_name})")

def fetch_and_store_weather_data(station_code: str, place_name: str ) -> pd.DataFrame:
    end_date = datetime.today().date()
    start_date = end_date - timedelta(days=365)
    all_observations = []

    for i in range(365):
        date = (start_date + timedelta(days=i)).strftime('%Y-%m-%d')
        url = f'https://api.meteo.lt/v1/stations/{station_code}/observations/{date}'
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            observations = data.get('observations', [])
            all_observations.extend(observations)
        else:
            print(f"Data for {date} not available (status code: {response.status_code})")

    df = pd.DataFrame(all_observations)
    if df.empty:
        print("No data retrieved.")
        return df

    # Set datetime index with UTC
    df['observationTimeUtc'] = pd.to_datetime(df['observationTimeUtc'], utc=True)
    df.set_index('observationTimeUtc', inplace=True)
    df.index = df.index.tz_convert(f'Europe/Vilnius')
    db_filename = f"his_{place_name}_{end_date}.db"
    conn = sqlite3.connect(db_filename)
    df.to_sql('observations', conn, if_exists='replace', index=True)  # suvienodinti his ir for *.db table names
    conn.close()

    print(f"Data saved to {db_filename}")
    return df   