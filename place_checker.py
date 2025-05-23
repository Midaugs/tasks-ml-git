import sqlite3
import datetime
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import math
import pandas as pd
import requests
#from datetime import datetime
import pytz

def save_user_query_to_db(user_input: str, db_path: str = "dbaze_re_placename.db"):
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS requestions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                query TEXT NOT NULL,
                timestamp TEXT NOT NULL
            )
        """)
        cursor.execute("""
            INSERT INTO requestions (query, timestamp)
            VALUES (?, ?)
        """, (user_input.strip(), datetime.datetime.now().isoformat()))
        conn.commit()

def check_place_exists(user_input: str, db_path: str = "gyvenamosios_vietoves.db") -> bool:
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM vietoves WHERE LOWER(pavadinimas) = LOWER(?)
        """, (user_input.strip(),))
        result = cursor.fetchone()
        return result[0] > 0

def get_lat_long(address):
    geolocator = Nominatim(user_agent="my_geocoder")
    try:
        location = geolocator.geocode(address, timeout=10)
        if location:
            ilguma = location.latitude
            platuma = location.longitude
            return (ilguma, platuma)
        else:
            return (None, None)
    except GeocoderTimedOut:
        return None

def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in kilometers
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = math.sin(delta_phi / 2)**2 + \
        math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c  # in kilometers

def find_nearest_ams_station(user_lat, user_lon, db_path="meteo_stations.db"):
  
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT code, latitude, longitude FROM stations")
        stations = cursor.fetchall()

        min_distance = float('inf')
        nearest_station = None

        for code, lat, lon in stations:
            dist = haversine(user_lat, user_lon, lat, lon)
            if dist < min_distance:
                min_distance = dist
                nearest_station = code

    return nearest_station, min_distance

def fetch_and_save_forecast(place_name: str, db_file: str):
    url = f"https://api.meteo.lt/v1/places/{place_name}/forecasts/long-term"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    forecasts = data['forecastTimestamps']
    df = pd.DataFrame(forecasts)

    # Convert forecastTimeUtc to datetime and localize
    df['forecastTimeUtc'] = pd.to_datetime(df['forecastTimeUtc'], utc=True)
    df.set_index('forecastTimeUtc', inplace=True)

    # Optionally localize to Lithuanian time (Europe/Vilnius)
    df.index = df.index.tz_convert(f'Europe/Vilnius')
    with sqlite3.connect(db_file) as conn:
        df.to_sql(f"{place_name}forecast", conn, if_exists="replace")  # pritaikyti visiems citys for_city_end_date.db table name.
    return df