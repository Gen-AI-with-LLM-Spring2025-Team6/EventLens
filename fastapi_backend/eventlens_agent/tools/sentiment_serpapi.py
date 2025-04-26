import requests
import os
import json
from typing import List, Optional, Dict, Tuple
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_core.messages import SystemMessage, HumanMessage

# Load environment variables
load_dotenv()

# Get API keys from environment variables
SERPAPI_API_KEY = os.getenv("SERPAPI_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Check if API keys are available
if not SERPAPI_API_KEY:
    raise ValueError("SerpAPI key is required. Set the SERPAPI_API_KEY environment variable.")

if not OPENAI_API_KEY:
    raise ValueError("OpenAI API key is required. Set the OPENAI_API_KEY environment variable.")


def get_event_reviews(event_name: str, location: Optional[str] = None, limit: int = 7) -> List[str]:
    """
    Extract top review texts for an event using SerpAPI.
    
    Args:
        event_name: Name of the event
        location: Optional location of the event
        limit: Maximum number of reviews to return (default: 7)
        
    Returns:
        List of review text snippets
    """
    # Construct the query
    query = event_name
    if location:
        query += f" {location}"
    query += " reviews experiences"
    
    # Set up the SerpAPI parameters
    params = {
        "api_key": SERPAPI_API_KEY,
        "engine": "google",
        "q": query,
        "num": 10,  # Request more than needed to ensure we get enough usable snippets
        "gl": "us",  # Country (Google locality)
        "hl": "en"   # Language
    }
    
    try:
        # Send the request to SerpAPI
        response = requests.get("https://serpapi.com/search", params=params)
        
        if response.status_code != 200:
            return [f"Error fetching reviews: HTTP {response.status_code}"]
        
        data = response.json()
        reviews = []
        
        # Extract organic results snippets
        if "organic_results" in data:
            for result in data["organic_results"]:
                if "snippet" in result and result["snippet"]:
                    reviews.append(result["snippet"])
                    
                    # Break if we have enough reviews
                    if len(reviews) >= limit:
                        break
        
        # If we still need more reviews, try extracting from "people also ask"
        if len(reviews) < limit and "related_questions" in data:
            for question in data["related_questions"]:
                if "snippet" in question and question["snippet"]:
                    reviews.append(question["snippet"])
                    
                    # Break if we have enough reviews
                    if len(reviews) >= limit:
                        break
        
        return reviews[:limit]  # Ensure we don't return more than the limit
        
    except Exception as e:
        return [f"Error: {str(e)}"]


def format_reviews_for_llm(event_name: str, location: Optional[str] = None, limit: int = 7) -> str:
    """
    Get reviews and format them for sending to another LLM.
    
    Args:
        event_name: Name of the event
        location: Optional location of the event
        limit: Maximum number of reviews to return
        
    Returns:
        Formatted string with reviews
    """
    reviews = get_event_reviews(event_name, location, limit)
    
    if not reviews:
        return f"No reviews found for {event_name}"
    
    # If the first review starts with "Error", it's an error message
    if reviews[0].startswith("Error"):
        return reviews[0]
    
    # Format the reviews
    event_info = event_name
    if location:
        event_info += f" in {location}"
        
    formatted = f"Reviews for {event_info}:\n\n"
    
    for i, review in enumerate(reviews, 1):
        formatted += f"Review #{i}: {review}\n\n"
    
    return formatted


def analyze_sentiment_with_llm(reviews: List[str]) -> Dict:
    """
    Use LLM to analyze sentiment of the reviews.
    
    Args:
        reviews: List of review text snippets
        
    Returns:
        Dict with sentiment analysis results
    """
    # Combine reviews into a single text for analysis
    reviews_text = "\n\n".join([f"Review: {review}" for review in reviews])
    
    # Initialize LLM with GPT-4o model
    llm = ChatOpenAI(model="gpt-4o", api_key=OPENAI_API_KEY)
    
    # Create system and user messages
    system_message = SystemMessage(content="""Analyze the sentiment of the provided reviews.
    Return a JSON object with the following structure:
    {
        "overall_sentiment": "positive" | "neutral" | "negative",
        "sentiment_score": number between -1.0 and 1.0,
        "positive_percentage": number between 0 and 100,
        "neutral_percentage": number between 0 and 100,
        "negative_percentage": number between 0 and 100,
        "key_positives": [list of 3 positive aspects mentioned],
        "key_negatives": [list of 3 negative aspects mentioned],
        "summary": "brief 1-2 sentence summary of overall sentiment"
    }
    
    Only return the JSON object, with no other text.
    """)
    
    user_message = HumanMessage(content=reviews_text)
    
    # Get the response from the LLM
    response = llm.invoke([system_message, user_message])
    
    try:
        # Parse the JSON response
        content = response.content
        cleaned = content.strip("```json\n").strip("```").strip()
        sentiment_results = json.loads(cleaned)
        return sentiment_results
    except:
        # If parsing fails, return a basic sentiment result
        return {
            "overall_sentiment": "neutral",
            "sentiment_score": 0,
            "positive_percentage": 0,
            "neutral_percentage": 100,
            "negative_percentage": 0,
            "key_positives": [],
            "key_negatives": [],
            "summary": "Unable to analyze sentiment from the reviews."
        }


def extract_event_from_query(query: str) -> Tuple[str, Optional[str]]:
    """
    Use LLM to extract event name and location from a user query.
    
    Args:
        query: The user's query text
        
    Returns:
        Tuple containing:
        - Event name
        - Optional location
    """
    # Initialize LLM with GPT-4o model
    llm = ChatOpenAI(model="gpt-4o", api_key=OPENAI_API_KEY)
    
    # Create system and user messages
    system_message = SystemMessage(content="""Extract the event name and location (if mentioned) from the user's query.
    Return a JSON object with the following structure:
    {
        "event_name": "extracted event name",
        "location": "extracted location or null if not mentioned"
    }
    
    Only return the JSON object, with no other text.
    """)
    
    user_message = HumanMessage(content=query)
    
    # Get the response from the LLM
    response = llm.invoke([system_message, user_message])
    
    try:
        # Parse the JSON response
        content = response.content
        cleaned = content.strip("```json\n").strip("```").strip()
        extraction_results = json.loads(cleaned)
        return extraction_results["event_name"], extraction_results.get("location")
    except:
        # If parsing fails, use the whole query as the event name
        return query, "Boston"  # Default to Boston as location


def process_sentiment_query(user_query: str) -> str:
    """
    Process a sentiment query from start to finish.
    This is the main entry point for the sentiment API.
    
    1. Takes the user's natural language query
    2. Extracts the event name and location using the LLM
    3. Gets reviews for the event
    4. Analyzes sentiment of the reviews
    5. Returns the formatted sentiment analysis
    
    Args:
        user_query: The user's natural language query about event sentiment
        
    Returns:
        Formatted sentiment analysis
    """
    try:
        # Step 1: Extract event name and location from the query
        event_name, location = extract_event_from_query(user_query)
        
        # Default to Boston if no location is specified
        if not location:
            location = "Boston"
        
        # Step 2: Get reviews for the event
        reviews = get_event_reviews(event_name, location)
        
        # Step 3: If we got an error message, return it
        if not reviews or reviews[0].startswith("Error"):
            return f"Could not find reviews for {event_name} in {location}"
        
        # Step 4: Analyze sentiment of the reviews
        sentiment = analyze_sentiment_with_llm(reviews)
        
        # Step 5: Format the sentiment analysis
        result = f"Sentiment Analysis for {event_name} in {location}:\n\n"
        result += f"Overall Sentiment: {sentiment['overall_sentiment'].capitalize()}\n"
        result += f"Sentiment Score: {sentiment['sentiment_score']:.2f} (-1.0 very negative to 1.0 very positive)\n\n"
        
        result += f"Positive: {sentiment['positive_percentage']:.1f}%\n"
        result += f"Neutral: {sentiment['neutral_percentage']:.1f}%\n"
        result += f"Negative: {sentiment['negative_percentage']:.1f}%\n\n"
        
        if sentiment["key_positives"]:
            result += "Key Positives:\n"
            for pos in sentiment["key_positives"]:
                result += f"- {pos}\n"
            result += "\n"
        
        if sentiment["key_negatives"]:
            result += "Key Negatives:\n"
            for neg in sentiment["key_negatives"]:
                result += f"- {neg}\n"
            result += "\n"
        
        result += f"Summary: {sentiment['summary']}\n\n"
        
        # Step 6: Add a few sample reviews
        result += "Sample Reviews:\n"
        for i, review in enumerate(reviews[:3], 1):  # Only include first 3 reviews
            result += f"Review #{i}: {review}\n\n"
        
        return result
        
    except Exception as e:
        return f"Error processing sentiment query: {str(e)}"


# For testing the API directly
#if __name__ == "__main__":
#    test_queries = [
#        "What do people think about the Red Sox games?"
#    ]
#    
#    for query in test_queries:
#        print(f"\nTEST QUERY: {query}")
#        print("-" * 50)
#        print(process_sentiment_query(query))
#        print("=" * 80)