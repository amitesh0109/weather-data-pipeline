# Weather Data Pipeline

A beginner-friendly data engineering project that collects, processes, and visualizes weather data. This project demonstrates key data engineering concepts including data extraction, transformation, loading (ETL), and visualization.

## Features

- Extract weather data from OpenWeatherMap API
- Store raw data in SQLite database
- Transform data to calculate useful metrics and aggregates
- Generate visualizations of weather trends and comparisons
- Scheduled data collection

## Project Structure

```
weather_data_pipeline/
├── config.py             # Configuration settings
├── extract.py            # Data extraction from API
├── transform.py          # Data transformation logic
├── load.py               # Database operations
├── visualize.py          # Create visualizations
├── main.py               # Main execution script
├── raw_data/             # Directory for raw JSON data
├── visualizations/       # Directory for generated charts
└── weather_data.db       # SQLite database
```

## Setup Instructions

1. Clone the repository or create the project structure as shown above

2. Create a virtual environment and install dependencies:
   ```bash
   python -m venv venv
   
   # On Windows:
   venv\Scripts\activate
   # On macOS/Linux:
   source venv/bin/activate
   
   pip install requests pandas matplotlib sqlite3 python-dotenv schedule
   ```

3. Sign up for a free API key from [OpenWeatherMap](https://openweathermap.org/)

4. Create a `.env` file with your API key:
   ```
   OPENWEATHERMAP_API_KEY=your_api_key_here
   ```

5. Customize the cities you want to track in `config.py`

## Running the Pipeline

To run the pipeline once:
```bash
python main.py
```

To run the pipeline on a schedule (specified in config.py):
```bash
SCHEDULE_PIPELINE=True python main.py
```

## Data Model

The project uses a SQLite database with the following main tables:

- `current_weather`: Current weather conditions for each city
- `forecast`: 5-day weather forecast data
- `daily_aggregates`: Daily aggregate metrics (min/max/avg temperatures)
- `temperature_anomalies`: Temperature deviations from average
- `extreme_weather_events`: Notable extreme weather conditions

## Visualizations

The pipeline generates several visualizations:

1. Temperature comparison charts across cities
2. Temperature range charts showing min/max/avg
3. City-specific temperature and humidity trends
4. Weather condition distribution
