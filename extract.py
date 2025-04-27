import os
import requests
import json
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('weather_extractor')

def load_api_key():
    """Load API key from environment variable or .env file"""
    try:
        from dotenv import load_dotenv
        load_dotenv()  # Load environment variables from .env file
    except ImportError:
        logger.warning("python-dotenv not installed. Assuming API key is set in environment variables.")
    
    api_key = os.getenv('OPENWEATHERMAP_API_KEY')
    if not api_key:
        raise ValueError("API key not found. Please set OPENWEATHERMAP_API_KEY environment variable.")
    return api_key

def get_current_weather(city, api_key):
    """
    Fetch current weather data for a specific city
    
    Args:
        city (str): City name (and optionally country code, e.g., "London,uk")
        api_key (str): OpenWeatherMap API key
        
    Returns:
        dict: Weather data or None if request failed
    """
    base_url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        'q': city,
        'appid': api_key,
        'units': 'metric'  # Use Celsius for temperature
    }
    
    try:
        logger.info(f"Fetching current weather data for {city}")
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Raise exception for HTTP errors
        
        data = response.json()
        # Add timestamp for when we retrieved the data
        data['retrieved_at'] = datetime.now().isoformat()
        
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data for {city}: {e}")
        return None

def get_forecast(city, api_key):
    """
    Fetch 5-day weather forecast data for a specific city
    
    Args:
        city (str): City name (and optionally country code, e.g., "London,uk")
        api_key (str): OpenWeatherMap API key
        
    Returns:
        dict: Forecast data or None if request failed
    """
    base_url = "https://api.openweathermap.org/data/2.5/forecast"
    params = {
        'q': city,
        'appid': api_key,
        'units': 'metric'  # Use Celsius for temperature
    }
    
    try:
        logger.info(f"Fetching forecast data for {city}")
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Raise exception for HTTP errors
        
        data = response.json()
        data['retrieved_at'] = datetime.now().isoformat()
        
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching forecast for {city}: {e}")
        return None

def save_raw_data(data, data_type, city):
    """
    Save raw data to JSON file
    
    Args:
        data (dict): Data to save
        data_type (str): Type of data (current or forecast)
        city (str): City name
    """
    # Create data directory if it doesn't exist
    os.makedirs('raw_data', exist_ok=True)
    
    # Clean city name for filename
    city_name = city.split(',')[0].lower().replace(' ', '_')
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"raw_data/{data_type}_{city_name}_{timestamp}.json"
    
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)
    
    logger.info(f"Saved raw {data_type} data to {filename}")
    return filename

def extract_weather_data(cities):
    """
    Main function to extract weather data for multiple cities
    
    Args:
        cities (list): List of city names
        
    Returns:
        dict: Dictionary containing the paths to saved data files
    """
    api_key = load_api_key()
    results = {
        'current': {},
        'forecast': {}
    }
    
    for city in cities:
        # Get current weather
        current_data = get_current_weather(city, api_key)
        if current_data:
            file_path = save_raw_data(current_data, 'current', city)
            results['current'][city] = file_path
        
        # Get forecast
        forecast_data = get_forecast(city, api_key)
        if forecast_data:
            file_path = save_raw_data(forecast_data, 'forecast', city)
            results['forecast'][city] = file_path
    
    return results

if __name__ == "__main__":
    # Example usage
    cities_to_fetch = ["London,uk", "New York,us", "Tokyo,jp"]
    extract_weather_data(cities_to_fetch)