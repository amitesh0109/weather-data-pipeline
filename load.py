import sqlite3
import json
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('weather_loader')

def init_database(db_path):
    """
    Initialize the SQLite database with the necessary tables
    
    Args:
        db_path (str): Path to the SQLite database file
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create table for current weather data
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS current_weather (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        city_id INTEGER,
        city_name TEXT,
        country TEXT,
        latitude REAL,
        longitude REAL,
        weather_main TEXT,
        weather_description TEXT,
        temperature REAL,
        feels_like REAL,
        temp_min REAL,
        temp_max REAL,
        pressure INTEGER,
        humidity INTEGER,
        wind_speed REAL,
        wind_direction INTEGER,
        cloudiness INTEGER,
        rain_1h REAL,
        snow_1h REAL,
        timestamp DATETIME,
        sunrise DATETIME,
        sunset DATETIME,
        retrieved_at DATETIME
    )
    ''')
    
    # Create table for forecast data
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS forecast (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        city_id INTEGER,
        city_name TEXT,
        country TEXT,
        forecast_time DATETIME,
        weather_main TEXT,
        weather_description TEXT,
        temperature REAL,
        feels_like REAL,
        temp_min REAL,
        temp_max REAL,
        pressure INTEGER,
        humidity INTEGER,
        wind_speed REAL,
        wind_direction INTEGER,
        cloudiness INTEGER,
        rain_3h REAL,
        snow_3h REAL,
        retrieved_at DATETIME
    )
    ''')
    
    # Create table for raw data files
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS raw_data_files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_path TEXT,
        data_type TEXT,
        city TEXT,
        timestamp DATETIME
    )
    ''')
    
    conn.commit()
    conn.close()
    
    logger.info(f"Database initialized at {db_path}")

def load_current_weather(file_path, db_path):
    """
    Load current weather data from a JSON file into the database
    
    Args:
        file_path (str): Path to the JSON file
        db_path (str): Path to the SQLite database
    """
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    # Connect to database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Extract values from JSON
    try:
        retrieved_at = data.get('retrieved_at')
        city_id = data.get('id')
        city_name = data.get('name')
        country = data.get('sys', {}).get('country')
        
        # Coordinates
        latitude = data.get('coord', {}).get('lat')
        longitude = data.get('coord', {}).get('lon')
        
        # Main weather data
        weather_main = data.get('weather', [{}])[0].get('main')
        weather_description = data.get('weather', [{}])[0].get('description')
        
        # Temperature data
        temperature = data.get('main', {}).get('temp')
        feels_like = data.get('main', {}).get('feels_like')
        temp_min = data.get('main', {}).get('temp_min')
        temp_max = data.get('main', {}).get('temp_max')
        pressure = data.get('main', {}).get('pressure')
        humidity = data.get('main', {}).get('humidity')
        
        # Wind data
        wind_speed = data.get('wind', {}).get('speed')
        wind_direction = data.get('wind', {}).get('deg')
        
        # Other data
        cloudiness = data.get('clouds', {}).get('all')
        rain_1h = data.get('rain', {}).get('1h', 0)
        snow_1h = data.get('snow', {}).get('1h', 0)
        
        # Convert Unix timestamps to datetime
        timestamp = datetime.fromtimestamp(data.get('dt', 0))
        sunrise = datetime.fromtimestamp(data.get('sys', {}).get('sunrise', 0))
        sunset = datetime.fromtimestamp(data.get('sys', {}).get('sunset', 0))
        
        # Insert data into the database
        cursor.execute('''
        INSERT INTO current_weather (
            city_id, city_name, country, latitude, longitude,
            weather_main, weather_description, temperature, feels_like,
            temp_min, temp_max, pressure, humidity, wind_speed,
            wind_direction, cloudiness, rain_1h, snow_1h,
            timestamp, sunrise, sunset, retrieved_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            city_id, city_name, country, latitude, longitude,
            weather_main, weather_description, temperature, feels_like,
            temp_min, temp_max, pressure, humidity, wind_speed,
            wind_direction, cloudiness, rain_1h, snow_1h,
            timestamp, sunrise, sunset, retrieved_at
        ))
        
        # Record the file in raw_data_files
        cursor.execute('''
        INSERT INTO raw_data_files (file_path, data_type, city, timestamp)
        VALUES (?, ?, ?, ?)
        ''', (file_path, 'current', city_name, datetime.now()))
        
        conn.commit()
        logger.info(f"Loaded current weather data for {city_name} into database")
        
    except Exception as e:
        logger.error(f"Error loading data from {file_path}: {e}")
        conn.rollback()
    
    conn.close()

def load_forecast_data(file_path, db_path):
    """
    Load forecast data from a JSON file into the database
    
    Args:
        file_path (str): Path to the JSON file
        db_path (str): Path to the SQLite database
    """
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    # Connect to database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        retrieved_at = data.get('retrieved_at')
        city_data = data.get('city', {})
        city_id = city_data.get('id')
        city_name = city_data.get('name')
        country = city_data.get('country')
        
        # Process each forecast time point
        for forecast in data.get('list', []):
            # Convert Unix timestamp to datetime
            forecast_time = datetime.fromtimestamp(forecast.get('dt', 0))
            
            weather_main = forecast.get('weather', [{}])[0].get('main')
            weather_description = forecast.get('weather', [{}])[0].get('description')
            
            # Temperature data
            temperature = forecast.get('main', {}).get('temp')
            feels_like = forecast.get('main', {}).get('feels_like')
            temp_min = forecast.get('main', {}).get('temp_min')
            temp_max = forecast.get('main', {}).get('temp_max')
            pressure = forecast.get('main', {}).get('pressure')
            humidity = forecast.get('main', {}).get('humidity')
            
            # Wind data
            wind_speed = forecast.get('wind', {}).get('speed')
            wind_direction = forecast.get('wind', {}).get('deg')
            
            # Other data
            cloudiness = forecast.get('clouds', {}).get('all')
            rain_3h = forecast.get('rain', {}).get('3h', 0)
            snow_3h = forecast.get('snow', {}).get('3h', 0)
            
            # Insert data into the database
            cursor.execute('''
            INSERT INTO forecast (
                city_id, city_name, country, forecast_time,
                weather_main, weather_description, temperature, feels_like,
                temp_min, temp_max, pressure, humidity, wind_speed,
                wind_direction, cloudiness, rain_3h, snow_3h, retrieved_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                city_id, city_name, country, forecast_time,
                weather_main, weather_description, temperature, feels_like,
                temp_min, temp_max, pressure, humidity, wind_speed,
                wind_direction, cloudiness, rain_3h, snow_3h, retrieved_at
            ))
        
        # Record the file in raw_data_files
        cursor.execute('''
        INSERT INTO raw_data_files (file_path, data_type, city, timestamp)
        VALUES (?, ?, ?, ?)
        ''', (file_path, 'forecast', city_name, datetime.now()))
        
        conn.commit()
        logger.info(f"Loaded forecast data for {city_name} into database")
        
    except Exception as e:
        logger.error(f"Error loading forecast data from {file_path}: {e}")
        conn.rollback()
    
    conn.close()

def load_data_files(results, db_path):
    """
    Load multiple data files into the database
    
    Args:
        results (dict): Dictionary with paths to data files as returned by extract_weather_data
        db_path (str): Path to the SQLite database
    """
    # Initialize database if it doesn't exist
    if not os.path.exists(db_path):
        init_database(db_path)
    
    # Load current weather data
    for city, file_path in results.get('current', {}).items():
        load_current_weather(file_path, db_path)
    
    # Load forecast data
    for city, file_path in results.get('forecast', {}).items():
        load_forecast_data(file_path, db_path)

if __name__ == "__main__":
    # Example usage
    db_path = "weather_data.db"
    init_database(db_path)
    # You would typically call load_data_files with results from extract_weather_data