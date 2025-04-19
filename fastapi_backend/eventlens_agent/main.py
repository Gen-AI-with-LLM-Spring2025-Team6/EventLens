#import os
#import sys
#from dotenv import load_dotenv
#
## Load environment variables
#load_dotenv()
#
## Try different import paths
#try:
#    # Import using full paths
#    from graph import event_recommendation_graph
#except ImportError:
#    try:
#        # Try relative import
#        from .graph import event_recommendation_graph
#    except ImportError:
#        print("ERROR: Unable to import event_recommendation_graph. Please check your project structure.")
#        sys.exit(1)
#
#def run_agent():
#    """
#    Run the EventLens agent with conversation memory.
#    """
#    print("="*80)
#    print("Welcome to EventLens! Ask me about events, weather, directions, or reviews.")
#    print("Type 'exit' or 'quit' to end the session.")
#    print("="*80)
#    
#    # Initialize conversation history
#    conversation_history = []
#    
#    while True:
#        user_input = input("\nYou: ")
#        
#        if user_input.lower() in ["exit", "quit"]:
#            print("\nThank you for using EventLens. Goodbye!")
#            break
#        
#        # Process the message with conversation history
#        response, conversation_history = event_recommendation_graph.invoke(user_input, conversation_history)
#        print(f"\nEventLens: {response}")
#
#if __name__ == "__main__":
#    # Run the agent
#    run_agent()

# fastapi_backend/fast_api/langgraph/main.py

from fastapi_backend.eventlens_agent.graph import event_recommendation_graph

def run_conversational_graph(message: str, history: list) -> str:
     response, conversation_history = event_recommendation_graph.invoke(message, history)
     return response

