import requests
import json
import os
from typing import Dict, Optional, Tuple
from datetime import datetime, timedelta
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage

# Load environment variables
load_dotenv()

# Get API keys from environment variables
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Check if API keys are available
if not OPENWEATHER_API_KEY:
    raise ValueError("OpenWeatherMap API key is required. Set the OPENWEATHER_API_KEY environment variable.")

if not OPENAI_API_KEY:
    raise ValueError("OpenAI API key is required. Set the OPENAI_API_KEY environment variable.")

class WeatherTool:
    """A tool for retrieving weather information using OpenWeatherMap API."""
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the WeatherTool.
        
        Args:
            api_key: OpenWeatherMap API key. If not provided, will try to get it from OPENWEATHER_API_KEY env variable.
        """
        self.api_key = api_key or OPENWEATHER_API_KEY
        
        if not self.api_key:
            raise ValueError(
                "OpenWeatherMap API key is required. Set the OPENWEATHER_API_KEY environment variable "
                "or pass it directly to the WeatherTool constructor."
            )
        
        self.base_url = "https://api.openweathermap.org/data/2.5"
        self.geocoding_url = "http://api.openweathermap.org/geo/1.0/direct"
    
    def geocode_address(self, address: str) -> Dict:
        """
        Convert a street address to latitude and longitude using OpenWeatherMap Geocoding API.
        
        Args:
            address: Full address string (e.g., '75 Saint Alphonsus Street, Boston, MA')
            
        Returns:
            Dict containing latitude and longitude
        """
        params = {
            "q": address,
            "limit": 1,
            "appid": self.api_key
        }
        
        response = requests.get(self.geocoding_url, params=params)
        
        if response.status_code != 200:
            error_msg = response.json().get("message", "Unknown error")
            raise Exception(f"Error geocoding address: {error_msg} (Code: {response.status_code})")
        
        results = response.json()
        
        if not results:
            raise Exception(f"No geocoding results found for address: {address}")
        
        location = results[0]
        return {
            "lat": location.get("lat"),
            "lon": location.get("lon"),
            "name": location.get("name"),
            "state": location.get("state", ""),
            "country": location.get("country", "")
        }
    
    def get_forecast_by_coords(self, lat: float, lon: float, units: str = "metric") -> Dict:
        """
        Get weather forecast by latitude and longitude.
        
        Args:
            lat: Latitude
            lon: Longitude
            units: Units of measurement. Options: 'standard', 'metric', or 'imperial'
            
        Returns:
            Dict containing forecast information
        """
        endpoint = f"{self.base_url}/forecast"
        params = {
            "lat": lat,
            "lon": lon,
            "appid": self.api_key,
            "units": units,
            "cnt": 40  # Maximum number of timestamps (5 days with 3-hour steps)
        }
        
        response = requests.get(endpoint, params=params)
        
        if response.status_code != 200:
            error_msg = response.json().get("message", "Unknown error")
            raise Exception(f"Error fetching forecast: {error_msg} (Code: {response.status_code})")
        
        return response.json()
    
    def format_forecast_for_date(self, forecast_data: Dict, date_str: str, 
                               units: str = "metric", is_today: bool = False) -> str:
        """
        Format forecast data for a specific date.
        
        Args:
            forecast_data: Forecast data from get_forecast_by_coords
            date_str: Date string in format 'YYYY-MM-DD'
            units: Units used in the data ('metric', 'imperial', 'standard')
            is_today: Flag indicating if the date is today's date
            
        Returns:
            Formatted forecast information for the specified date
        """
        temp_unit = "°C" if units == "metric" else "°F" if units == "imperial" else "K"
        city_name = forecast_data.get("city", {}).get("name", "Unknown location")
        city_country = forecast_data.get("city", {}).get("country", "")
        
        try:
            target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            
            # Format date for display
            date_display = date_str
            if is_today:
                date_display = f"today ({date_str})"
            
            # Filter forecasts for the target date
            forecasts = forecast_data.get("list", [])
            filtered_forecasts = [
                f for f in forecasts 
                if datetime.fromtimestamp(f.get("dt")).date() == target_date
            ]
            
            if not filtered_forecasts:
                return f"No forecast available for {date_display} at the specified location."
            
            # Format the response
            location_name = f"{city_name}, {city_country}" if city_country else city_name
            result = f"Weather forecast for {location_name} on {date_display}:\n\n"
            
            # Add each time period forecast
            for f in filtered_forecasts:
                dt = datetime.fromtimestamp(f.get("dt"))
                time_str = dt.strftime("%H:%M")
                
                temp = f.get("main", {}).get("temp", "N/A")
                feels_like = f.get("main", {}).get("feels_like", "N/A")
                humidity = f.get("main", {}).get("humidity", "N/A")
                description = f.get("weather", [{}])[0].get("description", "N/A")
                wind_speed = f.get("wind", {}).get("speed", "N/A")
                
                pop = f.get("pop", 0) * 100  # Probability of precipitation (as percentage)
                
                result += f"{time_str} - {description.capitalize()}\n"
                result += f"  Temperature: {temp}{temp_unit}, feels like {feels_like}{temp_unit}\n"
                result += f"  Humidity: {humidity}%, Wind: {wind_speed} m/s\n"
                result += f"  Chance of precipitation: {pop:.0f}%\n\n"
            
            # Add a summary
            avg_temp = sum(f.get("main", {}).get("temp", 0) for f in filtered_forecasts) / len(filtered_forecasts)
            avg_pop = sum(f.get("pop", 0) for f in filtered_forecasts) / len(filtered_forecasts) * 100
            
            # Count weather conditions
            conditions = {}
            for f in filtered_forecasts:
                condition = f.get("weather", [{}])[0].get("description", "N/A")
                conditions[condition] = conditions.get(condition, 0) + 1
            
            # Get the most common condition
            most_common_condition = max(conditions.items(), key=lambda x: x[1])[0]
            
            result += f"SUMMARY: {most_common_condition.capitalize()} for most of the day. "
            result += f"Average temperature: {avg_temp:.1f}{temp_unit}. "
            result += f"Chance of precipitation: {avg_pop:.0f}%."
            
            return result
            
        except ValueError:
            return f"Invalid date format. Please use YYYY-MM-DD."


def get_today_date_with_weekday() -> str:
    """
    Get today's date with weekday in a formatted string.
    
    Returns:
        String with today's date and weekday (e.g., "Thursday, April 17, 2025")
    """
    today = datetime.now()
    return today.strftime("%A, %B %d, %Y")


def extract_date_from_query(query: str) -> Tuple[str, bool]:
    """
    Use LLM to extract date from a user query.
    
    Args:
        query: The user's query text
        
    Returns:
        Tuple containing:
        - Date string in YYYY-MM-DD format
        - Boolean indicating if the date is today's date (no specific date in query)
    """
    # Get today's date with weekday using a Python function
    today_date_string = get_today_date_with_weekday()
    
    # Initialize LLM with GPT-4o model and the API key from environment
    llm = ChatOpenAI(model="gpt-4o", api_key=OPENAI_API_KEY)
    
    # Create system and user messages directly instead of using ChatPromptTemplate
    system_message = SystemMessage(content=f"""Extract the date mentioned in the user's query.
    Return a date in YYYY-MM-DD format.
    
    For relative dates like 'tomorrow', 'this weekend', 'next Friday', convert to an actual date.
    Today's date is {today_date_string}.
    
    If no specific date is mentioned, write 'TODAY'.
    
    Examples:
    "What's the weather tomorrow?" -> (converts to tomorrow's date in YYYY-MM-DD)
    "Will it rain this weekend?" -> (converts to this Saturday's date in YYYY-MM-DD)
    "Weather forecast for next Friday" -> (converts to next Friday's date in YYYY-MM-DD)
    "What's the weather like?" -> "TODAY"
    
    Return ONLY the date in YYYY-MM-DD format or "TODAY", with no other text.
    """)
    
    user_message = HumanMessage(content=query)
    
    # Get the response from the LLM
    response = llm.invoke([system_message, user_message])
    
    try:
        # Extract the date from the response
        date_str = response.content.strip()
        
        # Check if the response indicates today
        is_today = False
        if date_str == "TODAY":
            is_today = True
            date_str = datetime.now().strftime("%Y-%m-%d")
        else:
            # Validate the date format
            datetime.strptime(date_str, "%Y-%m-%d")
        
        return date_str, is_today
    except:
        # If parsing fails, return today's date
        return datetime.now().strftime("%Y-%m-%d"), True


def process_weather_query(user_query: str) -> str:
    """
    Process a weather query from start to finish.
    This is the main entry point for the weather API.
    
    1. Takes the user's natural language query
    2. Extracts the date using the LLM
    3. Gets the weather forecast for Boston on that date
    4. Returns the formatted forecast
    
    Args:
        user_query: The user's natural language query about weather
        
    Returns:
        Formatted weather forecast
    """
    try:
        # Step 1: Extract date from the query using LLM
        date, is_today = extract_date_from_query(user_query)
        
        # Step 2: Determine units from the query
        units = "metric"  # Default to metric (Celsius)
        if "fahrenheit" in user_query.lower() or " f " in user_query.lower():
            units = "imperial"  # Switch to imperial (Fahrenheit)
        
        # Step 3: Always use Boston as the location
        address = "Boston"
        
        # Step 4: Initialize the weather tool
        weather_tool = WeatherTool()
        
        # Step 5: Get coordinates for the location
        location = weather_tool.geocode_address(address)
        
        # Step 6: Get forecast data from the OpenWeather API
        forecast_data = weather_tool.get_forecast_by_coords(
            location["lat"], 
            location["lon"], 
            units=units
        )
        
        # Step 7: Format the forecast for the specific date
        return weather_tool.format_forecast_for_date(forecast_data, date, units, is_today)
        
    except Exception as e:
        return f"Error processing weather query: {str(e)}"


# For backward compatibility with your graph.py
def check_weather_for_address(address: str, date: str, units: str = "metric") -> str:
    """
    Original function for compatibility with existing code.
    
    Args:
        address: Full street address (e.g., '75 Saint Alphonsus Street, Boston, MA')
        date: Date in format 'YYYY-MM-DD'
        units: Units of measurement ('metric' for Celsius, 'imperial' for Fahrenheit)
        
    Returns:
        String with formatted weather forecast for the specified date
    """
    try:
        weather_tool = WeatherTool()
        
        # Convert address to coordinates
        location = weather_tool.geocode_address(address)
        
        # Get forecast data
        forecast_data = weather_tool.get_forecast_by_coords(
            location["lat"], 
            location["lon"], 
            units=units
        )
        
        # Check if date is today
        today_date = datetime.now().strftime("%Y-%m-%d")
        is_today = (date == today_date)
        
        # Format forecast for the specific date
        return weather_tool.format_forecast_for_date(forecast_data, date, units, is_today)
        
    except Exception as e:
        return f"Error retrieving weather information: {str(e)}"


# The following is just for testing the API directly
#if __name__ == "__main__":
#    # Get today's date dynamically
#    today_date_string = get_today_date_with_weekday()
#    print(f"Current date: {today_date_string}")
#    
#    # Test the API with user queries
#    test_queries = [
#        "I am planning to attend a event this sunday how is this weather?"
#    ]
#    
#    for query in test_queries:
#        print(f"\nTEST QUERY: {query}")
#        print(process_weather_query(query))