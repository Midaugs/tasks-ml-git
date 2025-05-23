import requests as rq
import pandas as pd
from placenames import Paklausimas
import pandas as pd
#from sqlalchemy import create_engine
from placenamesdatabase import Databaseentity
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from data_fetcher import fetch_gyvenamosios_vietoves, save_to_sqlite, fetch_meteo_stations, fetch_and_store_weather_data
from place_checker import save_user_query_to_db, check_place_exists, get_lat_long, find_nearest_ams_station, fetch_and_save_forecast
from data_visual import plot_actual_vs_forecast_scatter, interpolate_and_plot_5min_temperature, calculate_yearly_average_temperature, calculate_day_night_avg_temperature, count_rainy_weekend_days
from datetime import datetime, timedelta

def main():
    # url_ams_station = "https://api.meteo.lt/v1/stations"
    # user_request = Paklausimas()
    # data_from_database = Databaseentity()
    # json_file_path = 'https://get.data.gov.lt/datasets/gov/rc/ar/gyvenamojivietove/:all/:format/json'7
    # database_file_path = 'sqlite:///placenames.db'
    place_name = "Vilnius"
    end_date = datetime.today().date()
    while True:
        print('\nPasirinkite veiksmą: ')
        print('1 - Pabaiga')
        print('2 - Enter one word Lithuania place name (duomenų nuskaitymas API request historial and forecast)')  # Vartotojas įveda gyvenvietės pavadinimą.
        print('3 - Gyvenviečių nuskaitymas iš atvirų duomenų ir įrašymas į gyvenamosios_vietoves.db failą.')  # Gyvenviečių nuskaitymas iš atvirų duomenų ir įrašymas į *db failą.     
        print('4 - Meteorologinių stočių nuskaitymas iš https://api.meteo.lt/v1/stations ir įrašymas į meteo_stations.db failą.') # meteorologinių stočių nuskaitymas ir įrašymas į *.db failą.
        print('5 - Duomenų atvaizdavimas last week (measured) + next week (forecast)') 
        print('6 - Duomenų atvaizdavimas tarpinių reikšmių interpoliavimas')
        print('7 - Istoriniai duomenys vidutinė metų temperatūra ir oro drėgmė')
        print('8 - Istoriniai duomenys vidutinė metų dienos ir nakties temperatūra')
        print('9 - Istoriniai duomenys kiek savaitgalių lijo')
        print('10 - Duomenys')
        pasirinkimas = input('Iveskite pasirinkimo numeri:')

        if pasirinkimas == '1':
            print('Programos pabaiga.')
            break

        elif pasirinkimas == '2':  # Vartotojas įveda miestą, tikrinama su gyvenviečių duomenų baze, jei taip surandame artimiausią stotį, nuskaitome stoties duomenis nuo siandienos datos atgal metus, prognozės duomenys                  
            print("2. Enter query information")
            place_name = input("Lithunia place name: ")
            save_user_query_to_db(place_name)
            if check_place_exists(place_name):
                print(f"✅ '{place_name}' exists in the database.")
                user_lat, user_lon = get_lat_long(place_name)
                print('Jūsų įvestos gyvenvietės pavadinimas, ilguma, platuma: ', place_name, user_lat, user_lon)
                nearest_code, distance = find_nearest_ams_station(user_lat, user_lon)
                print(f"Nearest AMS station to {place_name}: {nearest_code} ({distance:.2f} km)") 
                print(f"Getting {place_name} forecast information please wait...")
                fetch_and_save_forecast(place_name, f'for_{place_name}_{end_date}.db')  # forecast prideti failo pavadinime datos trumpini
                print(f"Getting {place_name} meteorological observations please wait...")
                fetch_and_store_weather_data(nearest_code, place_name)
            else:
                print(f"❌ '{place_name}' not found in the database.")
                place_name = "Vilnius"

        elif pasirinkimas == '3':
            print("3. Jūs pasirinkote gyvenviečių nuskaitymas iš atvirų duomenų ir įrašymas į gyvenamosios_vietoves.db failą.")
            URL = "https://get.data.gov.lt/datasets/gov/rc/ar/gyvenamojivietove/:all/:format/json"
            DB_FILE = "gyvenamosios_vietoves.db"
            try:
                    df = fetch_gyvenamosios_vietoves(URL)
                    print(df.head())  # preview
                    save_to_sqlite(df, DB_FILE, "vietoves")
                    print('Duomenys sėkmingai įrašyti')
            except Exception as e:
                    print("❌ Error:", e)
     
        elif pasirinkimas == '4':
            print('4. meteorologinių stočių nuskaitymas iš https://api.meteo.lt/v1/stations ir įrašymas į meteo_stations.db failą.')
            URL = "https://api.meteo.lt/v1/stations"
            DB_FILE = "meteo_stations.db"
            try:
                df_stations = fetch_meteo_stations(URL)
                print(df_stations.head())  # Preview
                save_to_sqlite(df_stations, DB_FILE, "stations")
                print ("Duomenys sėkmingai įrašyti")
            except Exception as e:
                print("❌ Error fetching meteo stations:", e)

        elif pasirinkimas == '5':
             print('5 - Duomenų atvaizdavimas last week (measured) + next week (forecast)')
             plot_actual_vs_forecast_scatter(f'his_{place_name}_{end_date}.db', f'for_{place_name}_{end_date}.db', place_name)

        elif pasirinkimas == '6':
             interpolate_and_plot_5min_temperature(f'his_{place_name}_{end_date}.db', f'for_{place_name}_{end_date}.db', place_name)  # Kaip išspręsti su data, turim requestų database?

        elif pasirinkimas == '7':
             print(f'7 - {place_name}')
             calculate_yearly_average_temperature(f'his_{place_name}_{end_date}.db')

        elif pasirinkimas == '8':             
             print(f'8 - {place_name}')
             calculate_day_night_avg_temperature(f'his_{place_name}_{end_date}.db')

        elif pasirinkimas == '9':     
             print(f'9 - {place_name}')
             count_rainy_weekend_days(f'his_{place_name}_{end_date}.db')

        elif pasirinkimas =='10':
             print(f'10 Jūsų įvestas miestas(default - Vilnius) - {place_name}')
             print(f'10 Data (default today) - {end_date}, duomenų nuskaitymas historial ir forecast pasirinkus "2"')     

if __name__ == '__main__':    
      main()