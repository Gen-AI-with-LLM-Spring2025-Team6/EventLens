# fastapi_backend/fast_api/services/chat_service.py
from fastapi_backend.eventlens_agent.main import run_conversational_graph

def get_chatbot_response(message: str, history: list):
    return run_conversational_graph(message, history)
