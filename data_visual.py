import pandas as pd
import matplotlib.pyplot as plt
import sqlite3

def plot_actual_vs_forecast_scatter(hist_db_path: str, forecast_db_path: str, city:str):

    with sqlite3.connect(hist_db_path) as conn:
        df_hist = pd.read_sql_query("SELECT * FROM observations", conn)                     # vienodos table name visiems miestų failams
    df_hist['observationTimeUtc'] = pd.to_datetime(df_hist['observationTimeUtc'])
    last_date = df_hist['observationTimeUtc'].max()
    start_date = last_date - pd.Timedelta(days=6)
    df_hist_week = df_hist[df_hist['observationTimeUtc'] >= start_date].copy()
    df_hist_week = df_hist_week[['observationTimeUtc', 'airTemperature']]
    df_hist_week.rename(columns={'observationTimeUtc': 'datetime'}, inplace=True)
    with sqlite3.connect(forecast_db_path) as conn:
        df_forecast = pd.read_sql_query(f"SELECT * FROM {city}forecast", conn)
       # df_forecast = pd.read_sql_query("SELECT * FROM Vilniusforecast", conn)   # įhard kodinau vilnius, palikti tik forecast?? ar daryti vieną *.db su miestų table?
    df_forecast['forecastTimeUtc'] = pd.to_datetime(df_forecast['forecastTimeUtc'])
    start_forecast = df_forecast['forecastTimeUtc'].min()
    end_forecast = start_forecast + pd.Timedelta(days=6)
    df_forecast_week = df_forecast[
        (df_forecast['forecastTimeUtc'] >= start_forecast) &
        (df_forecast['forecastTimeUtc'] <= end_forecast)
    ].copy()
    df_forecast_week = df_forecast_week[['forecastTimeUtc', 'airTemperature']]
    df_forecast_week.rename(columns={'forecastTimeUtc': 'datetime'}, inplace=True)

    plt.figure(figsize=(12, 6))
    plt.scatter(df_hist_week['datetime'], df_hist_week['airTemperature'], color='green', label='Actual Temperature')
    plt.scatter(df_forecast_week['datetime'], df_forecast_week['airTemperature'], color='blue', label='Forecast Temperature')
    plt.xlabel('Date')
    plt.ylabel('Air Temperature (°C)')
    plt.title('Actual vs Forecast Temperature (Last Week vs First Forecast Week)')
    plt.legend()
    plt.grid(True)
    xticks = pd.date_range(start=df_hist_week['datetime'].min(), 
                           end=df_forecast_week['datetime'].max(), freq='D')
    plt.xticks(xticks, rotation=90)

    plt.tight_layout()
    plt.show()


def interpolate_and_plot_5min_temperature(hist_db_path: str, forecast_db_path: str, city: str):
    with sqlite3.connect(hist_db_path) as conn:
        df_hist = pd.read_sql_query("SELECT observationTimeUtc, airTemperature FROM observations", conn)
    df_hist['observationTimeUtc'] = pd.to_datetime(df_hist['observationTimeUtc'])
    last_date = df_hist['observationTimeUtc'].max()
    start_date = last_date - pd.Timedelta(days=6)
    df_hist_week = df_hist[df_hist['observationTimeUtc'] >= start_date].copy()
    df_hist_week.set_index('observationTimeUtc', inplace=True)
    with sqlite3.connect(forecast_db_path) as conn:
        df_forecast = pd.read_sql_query(f"SELECT forecastTimeUtc, airTemperature FROM {city}forecast", conn)
       # df_forecast = pd.read_sql_query("SELECT forecastTimeUtc, airTemperature FROM Vilniusforecast", conn) # įhard kodinau vilnius, palikti tik forecast?? ar daryti vieną *.db su miestų table?
    df_forecast['forecastTimeUtc'] = pd.to_datetime(df_forecast['forecastTimeUtc'])
    start_forecast = df_forecast['forecastTimeUtc'].min()
    end_forecast = start_forecast + pd.Timedelta(days=6)
    df_forecast_week = df_forecast[
        (df_forecast['forecastTimeUtc'] >= start_forecast) &
        (df_forecast['forecastTimeUtc'] <= end_forecast)
    ].copy()
    df_forecast_week.set_index('forecastTimeUtc', inplace=True)
    df_combined = pd.concat([df_hist_week, df_forecast_week])
    df_combined.sort_index(inplace=True)
    interpolated_5min = df_combined['airTemperature'].resample('5T').interpolate(method='polynomial', order=2)
    plt.figure(figsize=(12, 6))
    plt.scatter(interpolated_5min.index, interpolated_5min.values, s=10, color='red', alpha=0.6)
    plt.xlabel('Time')
    plt.ylabel('Air Temperature (°C)')
    plt.title('Interpolated Air Temperature (5-min Intervals, Polynomial Order 2)')
    plt.grid(True)
    plt.tight_layout()
    plt.show()
    return interpolated_5min  


def calculate_yearly_average_temperature(db_path: str):
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query("SELECT observationTimeUtc, airTemperature, relativeHumidity FROM observations", conn)
    df['observationTimeUtc'] = pd.to_datetime(df['observationTimeUtc'])
    df = df.sort_values('observationTimeUtc').reset_index(drop=True)
    average_temp = df['airTemperature'].mean()
    average_relative = df['relativeHumidity'].mean()
    first_time = df['observationTimeUtc'].iloc[0]
    last_time = df['observationTimeUtc'].iloc[-1]
    print(f"Average air temperature of the year: {average_temp:.2f} °C")
    print(f"Average relative humidity of the year: {average_relative:.2f} %")
    print(f"First observation time: {first_time}")
    print(f"Last observation time: {last_time}")
    return None


def calculate_day_night_avg_temperature(db_path: str):
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query("SELECT observationTimeUtc, airTemperature FROM observations", conn)
    df['observationTimeUtc'] = pd.to_datetime(df['observationTimeUtc'], utc=True)
    df['observationTimeUtc'] = df['observationTimeUtc'].dt.tz_localize(None)
    df = df.sort_values('observationTimeUtc').reset_index(drop=True)
    df['hour'] = df['observationTimeUtc'].dt.hour
    day_df = df[(df['hour'] >= 8) & (df['hour'] <= 20)]
    night_df = df[(df['hour'] <= 7) | (df['hour'] >= 21)]
    day_avg = day_df['airTemperature'].mean()
    night_avg = night_df['airTemperature'].mean()
    print(f"Average daytime air temperature: {day_avg:.2f} °C")
    print(f"Average nighttime air temperature: {night_avg:.2f} °C")
    return day_avg, night_avg


def count_rainy_weekend_days(db_path: str) -> int:
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query("SELECT observationTimeUtc, conditionCode FROM observations", conn)
    df['observationTimeUtc'] = pd.to_datetime(df['observationTimeUtc'], utc=True)
    df['observationTimeUtc'] = df['observationTimeUtc'].dt.tz_localize(None)
    df['date'] = df['observationTimeUtc'].dt.date
    df['weekday'] = df['observationTimeUtc'].dt.weekday  # 5 = Saturday, 6 = Sunday
    weekends = df[df['weekday'].isin([5, 6])]
    rain_conditions = {'rain'}
    rainy_weekends = weekends[weekends['conditionCode'].isin(rain_conditions)]
    rainy_weekend_days = rainy_weekends['date'].nunique()

    print(f"Number of weekend days with rain: {rainy_weekend_days}")
    return rainy_weekend_days