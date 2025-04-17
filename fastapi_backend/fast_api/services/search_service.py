from fastapi_backend.fast_api.services.rag_search_service import rag_event_search_pipeline

def search_events(query: str, model_name: str, api_key: str):
    return rag_event_search_pipeline(query, model_name, api_key)

