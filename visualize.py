import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger('weather_visualizer')

def set_plot_style(dark_mode=False):
    """
    Set the plot style based on dark mode preference
    
    Args:
        dark_mode (bool): Whether to use dark mode
    """
    if dark_mode:
        plt.style.use('dark_background')
    else:
        plt.style.use('ggplot')

def create_temperature_comparison_chart(db_path, output_dir, dark_mode=False):
    """
    Create a chart comparing temperatures across cities
    
    Args:
        db_path (str): Path to the SQLite database
        output_dir (str): Directory to save the chart
        dark_mode (bool): Whether to use dark mode
    """
    set_plot_style(dark_mode)
    
    conn = sqlite3.connect(db_path)
    
    # Get the daily aggregates
    query = """
    SELECT * FROM daily_aggregates
    ORDER BY city_name, date
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if df.empty:
        logger.warning("No daily aggregate data available for visualization")
        return
    
    # Convert date to datetime if needed
    if not pd.api.types.is_datetime64_any_dtype(df['date']):
        df['date'] = pd.to_datetime(df['date'])
    
    # Create a figure with enough height
    plt.figure(figsize=(12, 8))
    
    # Get unique cities
    cities = df['city_name'].unique()
    
    # Plot max temperature for each city
    for city in cities:
        city_data = df[df['city_name'] == city]
        plt.plot(city_data['date'], city_data['max_temp'], marker='o', linestyle='-', label=f"{city} (Max)")
    
    # Set labels and title
    plt.xlabel('Date')
    plt.ylabel('Temperature (°C)')
    plt.title('Maximum Daily Temperature Comparison')
    plt.grid(True, alpha=0.3)
    plt.legend()
    
    # Format x-axis date labels
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.gcf().autofmt_xdate()  # Rotate date labels
    
    # Save the figure
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = os.path.join(output_dir, f'temperature_comparison_{timestamp}.png')
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    plt.close()
    
    logger.info(f"Temperature comparison chart saved to {filename}")
    return filename

def create_temperature_range_chart(db_path, output_dir, dark_mode=False):
    """
    Create a chart showing temperature ranges (min to max) for each city
    
    Args:
        db_path (str): Path to the SQLite database
        output_dir (str): Directory to save the chart
        dark_mode (bool): Whether to use dark mode
    """
    set_plot_style(dark_mode)
    
    conn = sqlite3.connect(db_path)
    
    # Get the daily aggregates
    query = """
    SELECT * FROM daily_aggregates
    ORDER BY city_name, date
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if df.empty:
        logger.warning("No daily aggregate data available for visualization")
        return
    
    # Convert date to datetime if needed
    if not pd.api.types.is_datetime64_any_dtype(df['date']):
        df['date'] = pd.to_datetime(df['date'])
    
    # Create a figure
    plt.figure(figsize=(12, 10))
    
    # Get the most recent date in the data
    latest_date = df['date'].max()
    recent_data = df[df['date'] == latest_date]
    
    # Sort by average temperature
    recent_data = recent_data.sort_values('avg_temp')
    
    # Create a bar plot for temperature ranges
    cities = recent_data['city_name']
    y_pos = range(len(cities))
    
    # Create horizontal bars for temperature ranges
    plt.barh(y_pos, recent_data['max_temp'] - recent_data['min_temp'], 
             left=recent_data['min_temp'], height=0.6, color='skyblue', alpha=0.7,
             label='Temperature Range')
    
    # Add average temperature markers
    plt.scatter(recent_data['avg_temp'], y_pos, color='red', s=50, zorder=3, label='Average')
    
    # Set labels and title
    plt.yticks(y_pos, [f"{city} ({country})" for city, country in zip(recent_data['city_name'], recent_data['country'])])
    plt.xlabel('Temperature (°C)')
    plt.title(f'Temperature Ranges by City ({latest_date.strftime("%Y-%m-%d")})')
    plt.grid(True, alpha=0.3, axis='x')
    plt.legend()
    
    # Add temperature values as annotations
    for i, (min_temp, avg_temp, max_temp) in enumerate(zip(recent_data['min_temp'], 
                                                         recent_data['avg_temp'], 
                                                         recent_data['max_temp'])):
        plt.text(min_temp - 0.5, i, f"{min_temp:.1f}°C", va='center', ha='right', fontsize=9)
        plt.text(max_temp + 0.5, i, f"{max_temp:.1f}°C", va='center', ha='left', fontsize=9)
        plt.text(avg_temp, i + 0.3, f"{avg_temp:.1f}°C", va='bottom', ha='center', fontsize=9, color='darkred')
    
    # Save the figure
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = os.path.join(output_dir, f'temperature_range_{timestamp}.png')
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    plt.close()
    
    logger.info(f"Temperature range chart saved to {filename}")
    return filename

def create_temperature_trend_chart(db_path, city, output_dir, dark_mode=False):
    """
    Create a chart showing temperature trends for a specific city
    
    Args:
        db_path (str): Path to the SQLite database
        city (str): City name
        output_dir (str): Directory to save the chart
        dark_mode (bool): Whether to use dark mode
    """
    set_plot_style(dark_mode)
    
    conn = sqlite3.connect(db_path)
    
    # Get forecast data for the city
    query = f"""
    SELECT city_name, forecast_time, temperature, humidity
    FROM forecast
    WHERE city_name = '{city}'
    ORDER BY forecast_time
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if df.empty:
        logger.warning(f"No forecast data available for {city}")
        return
    
    # Convert forecast_time to datetime if needed
    if not pd.api.types.is_datetime64_any_dtype(df['forecast_time']):
        df['forecast_time'] = pd.to_datetime(df['forecast_time'])
    
    # Create a figure with two subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)
    
    # Plot temperature on the first subplot
    ax1.plot(df['forecast_time'], df['temperature'], marker='o', linestyle='-', color='tab:red')
    ax1.set_ylabel('Temperature (°C)')
    ax1.set_title(f'Temperature and Humidity Forecast for {city}')
    ax1.grid(True, alpha=0.3)
    
    # Plot humidity on the second subplot
    ax2.plot(df['forecast_time'], df['humidity'], marker='s', linestyle='-', color='tab:blue')
    ax2.set_ylabel('Humidity (%)')
    ax2.set_xlabel('Date and Time')
    ax2.grid(True, alpha=0.3)
    
    # Format x-axis date labels
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    fig.autofmt_xdate()  # Rotate date labels
    
    # Save the figure
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = os.path.join(output_dir, f'{city.lower().replace(" ", "_")}_trend_{timestamp}.png')
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    plt.close()
    
    logger.info(f"Temperature trend chart for {city} saved to {filename}")
    return filename

def create_weather_condition_chart(db_path, output_dir, dark_mode=False):
    """
    Create a chart showing the distribution of weather conditions
    
    Args:
        db_path (str): Path to the SQLite database
        output_dir (str): Directory to save the chart
        dark_mode (bool): Whether to use dark mode
    """
    set_plot_style(dark_mode)
    
    conn = sqlite3.connect(db_path)
    
    # Get weather conditions data
    query = """
    SELECT city_name, weather_main, COUNT(*) as count
    FROM forecast
    GROUP BY city_name, weather_main
    ORDER BY city_name, count DESC
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if df.empty:
        logger.warning("No forecast data available for visualization")
        return
    
    # Create a figure
    plt.figure(figsize=(14, 8))
    
    # Get unique cities and weather conditions
    cities = df['city_name'].unique()
    weather_conditions = df['weather_main'].unique()
    
    # Prepare data for stacked bar chart
    data = {}
    for condition in weather_conditions:
        data[condition] = []
        for city in cities:
            city_condition_data = df[(df['city_name'] == city) & (df['weather_main'] == condition)]
            count = city_condition_data['count'].sum() if not city_condition_data.empty else 0
            data[condition].append(count)
    
    # Create stacked bar chart
    bottom = [0] * len(cities)
    for condition in weather_conditions:
        plt.bar(cities, data[condition], bottom=bottom, label=condition)
        bottom = [b + d for b, d in zip(bottom, data[condition])]
    
    # Set labels and title
    plt.xlabel('City')
    plt.ylabel('Count')
    plt.title('Distribution of Weather Conditions by City')
    plt.xticks(rotation=45, ha='right')
    plt.legend(title='Weather Condition')
    plt.grid(True, alpha=0.3, axis='y')
    plt.tight_layout()
    
    # Save the figure
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = os.path.join(output_dir, f'weather_conditions_{timestamp}.png')
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    plt.close()
    
    logger.info(f"Weather conditions chart saved to {filename}")
    return filename

def create_all_visualizations(db_path, output_dir, dark_mode=False):
    """
    Create all visualizations
    
    Args:
        db_path (str): Path to the SQLite database
        output_dir (str): Directory to save the charts
        dark_mode (bool): Whether to use dark mode
    """
    logger.info("Starting visualization creation")
    
    # Create directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        # Create temperature comparison chart
        create_temperature_comparison_chart(db_path, output_dir, dark_mode)
        
        # Create temperature range chart
        create_temperature_range_chart(db_path, output_dir, dark_mode)
        
        # Get list of cities from database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT city_name FROM forecast")
        cities = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        # Create temperature trend charts for each city
        for city in cities:
            create_temperature_trend_chart(db_path, city, output_dir, dark_mode)
        
        # Create weather condition chart
        create_weather_condition_chart(db_path, output_dir, dark_mode)
        
        logger.info("All visualizations created successfully")
        
    except Exception as e:
        logger.error(f"Error creating visualizations: {e}")
        raise

if __name__ == "__main__":
    # Example usage
    db_path = "weather_data.db"
    output_dir = "visualizations"
    dark_mode = False
    create_all_visualizations(db_path, output_dir, dark_mode)