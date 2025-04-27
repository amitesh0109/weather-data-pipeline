import sqlite3
import pandas as pd
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('weather_transformer')

def get_current_weather_data(db_path):
    """
    Retrieve current weather data from the database and convert to DataFrame
    
    Args:
        db_path (str): Path to the SQLite database
        
    Returns:
        DataFrame: Current weather data
    """
    conn = sqlite3.connect(db_path)
    
    query = """
    SELECT * FROM current_weather
    ORDER BY city_name, timestamp
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    # Convert timestamp columns to datetime
    for col in ['timestamp', 'sunrise', 'sunset', 'retrieved_at']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
            
    return df

def get_forecast_data(db_path):
    """
    Retrieve forecast data from the database and convert to DataFrame
    
    Args:
        db_path (str): Path to the SQLite database
        
    Returns:
        DataFrame: Forecast data
    """
    conn = sqlite3.connect(db_path)
    
    query = """
    SELECT * FROM forecast
    ORDER BY city_name, forecast_time
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    # Convert timestamp columns to datetime
    for col in ['forecast_time', 'retrieved_at']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
            
    return df

def calculate_daily_aggregates(db_path):
    """
    Calculate daily temperature aggregates from forecast data
    
    Args:
        db_path (str): Path to the SQLite database
        
    Returns:
        DataFrame: Daily temperature aggregates
    """
    conn = sqlite3.connect(db_path)
    
    query = """
    SELECT 
        city_name,
        country,
        date(forecast_time) as date,
        avg(temperature) as avg_temp,
        min(temperature) as min_temp,
        max(temperature) as max_temp,
        avg(humidity) as avg_humidity,
        avg(wind_speed) as avg_wind_speed
    FROM forecast
    GROUP BY city_name, country, date(forecast_time)
    ORDER BY city_name, date
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    # Convert date to datetime
    df['date'] = pd.to_datetime(df['date'])
    
    return df

def calculate_temperature_anomalies(db_path):
    """
    Calculate temperature anomalies compared to average for that city
    
    Args:
        db_path (str): Path to the SQLite database
        
    Returns:
        DataFrame: Temperature anomalies
    """
    # Get forecast data
    forecast_df = get_forecast_data(db_path)
    
    # Calculate average temperature per city
    city_avg_temp = forecast_df.groupby('city_name')['temperature'].mean().reset_index()
    city_avg_temp.rename(columns={'temperature': 'city_avg_temp'}, inplace=True)
    
    # Merge with original data
    merged_df = pd.merge(forecast_df, city_avg_temp, on='city_name')
    
    # Calculate anomaly
    merged_df['temp_anomaly'] = merged_df['temperature'] - merged_df['city_avg_temp']
    
    return merged_df[['city_name', 'country', 'forecast_time', 'temperature', 'city_avg_temp', 'temp_anomaly']]

def find_extreme_weather_events(db_path):
    """
    Identify extreme weather events in the forecast data
    
    Args:
        db_path (str): Path to the SQLite database
        
    Returns:
        DataFrame: Extreme weather events
    """
    # Get forecast data
    forecast_df = get_forecast_data(db_path)
    
    # Define extreme weather conditions
    extreme_conditions = (
        # Extreme temperatures (below -10°C or above 40°C)
        (forecast_df['temperature'] < -10) | 
        (forecast_df['temperature'] > 40) |
        # Strong winds (above 20 m/s, approximately 72 km/h)
        (forecast_df['wind_speed'] > 20) |
        # Heavy rain (>10mm in 3 hours)
        (forecast_df['rain_3h'] > 10) |
        # Heavy snow (>10mm in 3 hours)
        (forecast_df['snow_3h'] > 10)
    )
    
    # Filter for extreme weather events
    extreme_events = forecast_df[extreme_conditions].copy()
    
    # Create a description column
    conditions = []
    for _, row in extreme_events.iterrows():
        event_descriptions = []
        if row['temperature'] < -10:
            event_descriptions.append(f"Extreme cold: {row['temperature']:.1f}°C")
        if row['temperature'] > 40:
            event_descriptions.append(f"Extreme heat: {row['temperature']:.1f}°C")
        if row['wind_speed'] > 20:
            event_descriptions.append(f"Strong winds: {row['wind_speed']:.1f} m/s")
        if row['rain_3h'] > 10:
            event_descriptions.append(f"Heavy rain: {row['rain_3h']:.1f} mm/3h")
        if row['snow_3h'] > 10:
            event_descriptions.append(f"Heavy snow: {row['snow_3h']:.1f} mm/3h")
        
        conditions.append("; ".join(event_descriptions))
    
    extreme_events['event_description'] = conditions
    
    return extreme_events

def store_transformed_data(df, table_name, db_path):
    """
    Store transformed data back to the database
    
    Args:
        df (DataFrame): Data to store
        table_name (str): Name of the table
        db_path (str): Path to the SQLite database
    """
    conn = sqlite3.connect(db_path)
    
    # Check if table exists and create it if it doesn't
    cursor = conn.cursor()
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    if not cursor.fetchone():
        logger.info(f"Creating table {table_name}")
        df.to_sql(table_name, conn, index=False)
    else:
        # Table exists, use if_exists='append'
        df.to_sql(table_name, conn, if_exists='append', index=False)
    
    conn.close()
    logger.info(f"Stored transformed data in table {table_name}")

def run_transformations(db_path):
    """
    Run all transformations
    
    Args:
        db_path (str): Path to the SQLite database
    """
    logger.info("Starting data transformations")
    
    try:
        # Calculate daily aggregates
        daily_agg = calculate_daily_aggregates(db_path)
        store_transformed_data(daily_agg, 'daily_aggregates', db_path)
        
        # Calculate temperature anomalies
        temp_anomalies = calculate_temperature_anomalies(db_path)
        store_transformed_data(temp_anomalies, 'temperature_anomalies', db_path)
        
        # Find extreme weather events
        extreme_events = find_extreme_weather_events(db_path)
        if not extreme_events.empty:
            store_transformed_data(extreme_events, 'extreme_weather_events', db_path)
        
        logger.info("Data transformations completed successfully")
        
    except Exception as e:
        logger.error(f"Error during data transformations: {e}")
        raise

if __name__ == "__main__":
    # Example usage
    db_path = "weather_data.db"
    run_transformations(db_path)