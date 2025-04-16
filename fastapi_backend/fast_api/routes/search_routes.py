from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from fastapi_backend.fast_api.services.search_service import search_events
from fastapi_backend.fast_api.services.auth_service import get_current_user
from dotenv import load_dotenv
import os


load_dotenv()

router = APIRouter()

class SearchQuery(BaseModel):
    query: str

@router.post("/search")
async def search_event_handler(
    payload: SearchQuery,
    user=Depends(get_current_user)
):
    try:
        query = payload.query
        model_name = "gpt-4o"
        api_key = os.getenv("OPENAI_API_KEY") 

        if not api_key:
            raise HTTPException(status_code=401, detail="Missing OpenAI API key")

        results = search_events(query, model_name, api_key)
        return {"events": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
