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
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Check if API keys are available
if not GOOGLE_MAPS_API_KEY:
    raise ValueError("Google Maps API key is required. Set the GOOGLE_MAPS_API_KEY environment variable.")

if not OPENAI_API_KEY:
    raise ValueError("OpenAI API key is required. Set the OPENAI_API_KEY environment variable.")

class MapsDirectionsTool:
    """A tool for retrieving directions and travel information using Google Maps API."""
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the MapsDirectionsTool.
        
        Args:
            api_key: Google Maps API key. If not provided, will try to get it from GOOGLE_MAPS_API_KEY env variable.
        """
        self.api_key = api_key or GOOGLE_MAPS_API_KEY
        
        if not self.api_key:
            raise ValueError(
                "Google Maps API key is required. Set the GOOGLE_MAPS_API_KEY environment variable "
                "or pass it directly to the MapsDirectionsTool constructor."
            )
        
        self.directions_url = "https://maps.googleapis.com/maps/api/directions/json"
    
    def get_directions(self, 
                      origin: str, 
                      destination: str, 
                      mode: str = "driving", 
                      departure_time: Optional[str] = None,
                      arrival_time: Optional[str] = None) -> Dict:
        """
        Get directions from origin to destination.
        
        Args:
            origin: Starting address or location
            destination: Ending address or location
            mode: Mode of transportation ('driving', 'walking', 'bicycling', 'transit')
            departure_time: Optional departure time in ISO format (YYYY-MM-DDTHH:MM:SS)
            arrival_time: Optional arrival time in ISO format (YYYY-MM-DDTHH:MM:SS)
            
        Returns:
            Dict containing directions information
        """
        params = {
            "origin": origin,
            "destination": destination,
            "mode": mode,
            "key": self.api_key
        }
        
        # Convert departure_time to UNIX timestamp if provided
        if departure_time:
            try:
                dt = datetime.fromisoformat(departure_time)
                params["departure_time"] = int(dt.timestamp())
            except ValueError:
                raise ValueError(f"Invalid departure_time format. Expected ISO format (YYYY-MM-DDTHH:MM:SS), got {departure_time}")
        
        # Convert arrival_time to UNIX timestamp if provided
        if arrival_time:
            try:
                at = datetime.fromisoformat(arrival_time)
                params["arrival_time"] = int(at.timestamp())
            except ValueError:
                raise ValueError(f"Invalid arrival_time format. Expected ISO format (YYYY-MM-DDTHH:MM:SS), got {arrival_time}")
        
        # For transit mode, we need either departure_time or arrival_time
        if mode == "transit" and not (departure_time or arrival_time):
            # Use current time as default departure time
            params["departure_time"] = int(datetime.now().timestamp())
        
        # Add alternatives to get multiple route options
        params["alternatives"] = "true"
        
        response = requests.get(self.directions_url, params=params)
        
        if response.status_code != 200:
            error_msg = response.json().get("error_message", "Unknown error")
            raise Exception(f"Error fetching directions: {error_msg} (Code: {response.status_code})")
        
        result = response.json()
        
        if result["status"] != "OK":
            error_msg = result.get("error_message", result["status"])
            raise Exception(f"Error fetching directions: {error_msg}")
        
        return result
    
    def extract_route_info(self, directions_data: Dict, route_index: int = 0) -> Dict:
        """
        Extract useful information from a specific route in the directions data.
        
        Args:
            directions_data: Data returned from get_directions
            route_index: Index of the route to extract info from (default: 0 for primary route)
            
        Returns:
            Dict with simplified route information
        """
        if not directions_data.get("routes"):
            raise ValueError("No routes found in directions data")
        
        if route_index >= len(directions_data["routes"]):
            raise ValueError(f"Route index {route_index} out of range. Only {len(directions_data['routes'])} routes available.")
        
        route = directions_data["routes"][route_index]
        legs = route["legs"]
        
        # Extract overall distance and duration
        total_distance = sum(leg["distance"]["value"] for leg in legs)  # in meters
        total_duration = sum(leg["duration"]["value"] for leg in legs)  # in seconds
        
        # Check if there's traffic duration information
        has_traffic_info = any("duration_in_traffic" in leg for leg in legs)
        if has_traffic_info:
            total_duration_in_traffic = sum(leg.get("duration_in_traffic", {}).get("value", leg["duration"]["value"]) for leg in legs)
        else:
            total_duration_in_traffic = None
        
        # Extract start and end addresses
        start_address = legs[0]["start_address"]
        end_address = legs[-1]["end_address"]
        
        # Extract steps for navigation
        steps = []
        for leg_index, leg in enumerate(legs):
            for step in leg["steps"]:
                step_info = {
                    "instruction": step["html_instructions"],
                    "distance": step["distance"]["text"],
                    "duration": step["duration"]["text"],
                }
                
                # Add transit-specific details if available
                if step.get("transit_details"):
                    transit = step["transit_details"]
                    step_info["transit"] = {
                        "line": transit.get("line", {}).get("name", "Unknown line"),
                        "departure_stop": transit.get("departure_stop", {}).get("name", "Unknown"),
                        "arrival_stop": transit.get("arrival_stop", {}).get("name", "Unknown"),
                        "num_stops": transit.get("num_stops", "Unknown"),
                        "departure_time": transit.get("departure_time", {}).get("text", "Unknown"),
                        "arrival_time": transit.get("arrival_time", {}).get("text", "Unknown"),
                    }
                
                steps.append(step_info)
        
        # Get polyline for the route
        route_polyline = route["overview_polyline"]["points"]
        
        # Return simplified route info
        return {
            "start_address": start_address,
            "end_address": end_address,
            "distance": {
                "text": self._format_distance(total_distance),
                "value": total_distance
            },
            "duration": {
                "text": self._format_duration(total_duration),
                "value": total_duration
            },
            "duration_in_traffic": {
                "text": self._format_duration(total_duration_in_traffic) if total_duration_in_traffic else None,
                "value": total_duration_in_traffic
            } if has_traffic_info else None,
            "steps": steps,
            "polyline": route_polyline,
        }
    
    def format_directions_response(self, route_info: Dict, include_steps: bool = True, 
                                  mode: str = "driving") -> str:
        """
        Format the route information into a human-readable string.
        
        Args:
            route_info: Route information from extract_route_info
            include_steps: Whether to include step-by-step directions
            mode: Mode of transportation used
            
        Returns:
            Formatted directions text
        """
        # Start with summary information
        result = f"Directions from {route_info['start_address']} to {route_info['end_address']}:\n\n"
        
        # Add mode of transportation
        mode_display = {
            "driving": "Driving",
            "walking": "Walking",
            "bicycling": "Bicycling",
            "transit": "Public Transit"
        }.get(mode, mode.capitalize())
        
        result += f"Mode: {mode_display}\n"
        result += f"Total Distance: {route_info['distance']['text']}\n"
        result += f"Estimated Travel Time: {route_info['duration']['text']}\n"
        
        # Add traffic information if available
        if route_info.get("duration_in_traffic"):
            result += f"Travel Time with Traffic: {route_info['duration_in_traffic']['text']}\n"
        
        # Add URL to Google Maps
        start_encoded = requests.utils.quote(route_info['start_address'])
        end_encoded = requests.utils.quote(route_info['end_address'])
        maps_url = f"https://www.google.com/maps/dir/?api=1&origin={start_encoded}&destination={end_encoded}&travelmode={mode}"
        result += f"\nView on Google Maps: {maps_url}\n"
        
        # Add step-by-step directions if requested
        if include_steps and route_info.get("steps"):
            result += "\nStep-by-Step Directions:\n"
            
            for i, step in enumerate(route_info["steps"], 1):
                # Clean up HTML tags in instructions (simple approach)
                instruction = step["instruction"].replace("<b>", "").replace("</b>", "")
                instruction = instruction.replace("<div>", "\n  ").replace("</div>", "")
                
                result += f"{i}. {instruction} ({step['distance']} - {step['duration']})\n"
                
                # Add transit details if available
                if step.get("transit"):
                    transit = step["transit"]
                    result += f"   Take {transit['line']} from {transit['departure_stop']} to {transit['arrival_stop']}\n"
                    result += f"   Departure: {transit['departure_time']}, Arrival: {transit['arrival_time']}\n"
                    result += f"   {transit['num_stops']} stops\n"
                
        return result
    
    def _format_distance(self, distance_in_meters: float) -> str:
        """Format distance in a human-readable way."""
        if distance_in_meters < 1000:
            return f"{distance_in_meters:.0f} m"
        else:
            return f"{distance_in_meters/1000:.1f} km"
    
    def _format_duration(self, duration_in_seconds: float) -> str:
        """Format duration in a human-readable way."""
        if not duration_in_seconds:
            return "Unknown"
            
        hours, remainder = divmod(int(duration_in_seconds), 3600)
        minutes, seconds = divmod(remainder, 60)
        
        parts = []
        if hours > 0:
            parts.append(f"{hours} hr")
        if minutes > 0 or not parts:  # Always show minutes if there are no hours
            parts.append(f"{minutes} min")
            
        return " ".join(parts)


def extract_directions_parameters(query: str) -> Dict:
    """
    Use LLM to extract directions parameters from a user query.
    
    Args:
        query: The user's query text
        
    Returns:
        Dict with extracted parameters (origin, destination, mode, etc.)
    """
    # Initialize LLM with GPT-4o model
    llm = ChatOpenAI(model="gpt-4o", api_key=OPENAI_API_KEY)
    
    # Create system and user messages
    system_message = SystemMessage(content="""Extract directions parameters from the user's query.
    Return a JSON object with the following structure:
    {
        "origin": "starting location (if mentioned, otherwise null)",
        "destination": "destination location (must be present)",
        "mode": "mode of transportation (driving, walking, bicycling, transit) or null if not specified",
        "departure_time": "departure time in ISO format (YYYY-MM-DDTHH:MM:SS) or null if not specified",
        "arrival_time": "arrival time in ISO format (YYYY-MM-DDTHH:MM:SS) or null if not specified"
    }
    
    Today's date is Thursday, April 17, 2025.
    
    For relative times like "in 2 hours" or "tomorrow at 3pm", convert to an actual ISO datetime.
    For locations in Boston, add "Boston, MA" if only a street or landmark is given.
    If no origin is specified, return null for origin.
    
    Only return the JSON object, with no other text.
    """)
    
    user_message = HumanMessage(content=query)
    
    # Get the response from the LLM
    response = llm.invoke([system_message, user_message])
    
    try:
        # Parse the JSON response
        parameters = json.loads(response.content)
        
        # Default mode to "driving" if not specified
        if not parameters.get("mode"):
            parameters["mode"] = "driving"
            
        return parameters
    except:
        # If parsing fails, extract basic destination and use defaults
        return {
            "origin": None,
            "destination": query,
            "mode": "driving",
            "departure_time": None,
            "arrival_time": None
        }


def process_directions_query(user_query: str) -> str:
    """
    Process a directions query from start to finish.
    This is the main entry point for the directions API.
    
    1. Takes the user's natural language query
    2. Extracts the origin, destination, and other parameters using the LLM
    3. Gets directions from Google Maps API
    4. Returns the formatted directions
    
    Args:
        user_query: The user's natural language query about directions
        
    Returns:
        Formatted directions
    """
    try:
        # Step 1: Extract parameters from the query
        params = extract_directions_parameters(user_query)
        
        # Step 2: Check if we have a destination (required)
        if not params.get("destination"):
            return "I couldn't determine where you want to go. Please specify a destination."
        
        # Step 3: Default to Boston if no origin is provided
        origin = params.get("origin") or "Boston, MA"
        
        # Step 4: Use the extracted parameters to get directions
        mode = params.get("mode", "driving").lower()
        
        # Step 5: Validate the mode
        valid_modes = ["driving", "walking", "bicycling", "transit"]
        if mode not in valid_modes:
            # Default to driving if invalid mode
            mode = "driving"
        
        # Step 6: If mode is specified as "public", convert to "transit" for Google Maps API
        if mode == "public":
            mode = "transit"
            
        # Step 7: Initialize the Maps tool
        maps_tool = MapsDirectionsTool()
        
        # Step 8: Get directions data
        directions_data = maps_tool.get_directions(
            origin=origin,
            destination=params["destination"],
            mode=mode,
            departure_time=params.get("departure_time"),
            arrival_time=params.get("arrival_time")
        )
        
        # Step 9: Extract route information
        route_info = maps_tool.extract_route_info(directions_data)
        
        # Step 10: Format the response
        return maps_tool.format_directions_response(route_info, include_steps=True, mode=mode)
        
    except Exception as e:
        return f"Error processing directions query: {str(e)}"


# For backward compatibility with your graph.py
def get_directions(origin: str, destination: str, mode: str = "driving", 
                  departure_time: Optional[str] = None, arrival_time: Optional[str] = None,
                  include_steps: bool = True) -> str:
    """
    Get directions between two locations.
    
    Args:
        origin: Starting address or location
        destination: Ending address or location
        mode: Mode of transportation ('driving', 'walking', 'bicycling', 'transit')
        departure_time: Optional departure time in ISO format (YYYY-MM-DDTHH:MM:SS)
        arrival_time: Optional arrival time in ISO format (YYYY-MM-DDTHH:MM:SS)
        include_steps: Whether to include step-by-step directions
        
    Returns:
        String with formatted directions information
    """
    try:
        # Validate mode
        valid_modes = ["driving", "walking", "bicycling", "transit"]
        if mode not in valid_modes:
            return f"Invalid mode: {mode}. Please use one of {', '.join(valid_modes)}."
        
        # If mode is specified as "public", convert to "transit" for Google Maps API
        if mode.lower() == "public":
            mode = "transit"
            
        maps_tool = MapsDirectionsTool()
        
        # Get directions data
        directions_data = maps_tool.get_directions(
            origin=origin,
            destination=destination,
            mode=mode,
            departure_time=departure_time,
            arrival_time=arrival_time
        )
        
        # Extract route information
        route_info = maps_tool.extract_route_info(directions_data)
        
        # Format the response
        return maps_tool.format_directions_response(route_info, include_steps, mode)
        
    except Exception as e:
        return f"Error retrieving directions: {str(e)}"


# For testing the API directly
#if __name__ == "__main__":
#    # Test with different query types
#    test_queries = [
#        "Give me directions from 75 Saint Alphonsus Street to Downtown Crossing using public transit"
#    ]
#    
#    for query in test_queries:
#        print(f"\nTEST QUERY: {query}")
#        print("-" * 50)
#        print(process_directions_query(query))
#        print("=" * 80)