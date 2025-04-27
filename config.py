"""
Configuration settings for the weather data pipeline
"""

# List of cities to fetch weather data for
CITIES = [
    "London,uk", 
    "New York,us", 
    "Tokyo,jp",
    "Sydney,au",
    "Paris,fr"
]

# Database settings
DATABASE_PATH = "weather_data.db"

# Data collection frequency (in hours)
COLLECTION_FREQUENCY = 3

# Visualization settings
VISUALIZATION_OUTPUT_DIR = "visualizations"

# Set this to True to use dark theme for plots
DARK_MODE = False