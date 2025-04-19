from fastapi import APIRouter, HTTPException, Depends, Request
from pydantic import BaseModel
from dotenv import load_dotenv
import os

from fastapi_backend.fast_api.services.search_service import search_events
from fastapi_backend.fast_api.services.auth_service import get_current_user
from fastapi_backend.fast_api.utils.log_search import log_user_search

load_dotenv()

router = APIRouter()

class SearchQuery(BaseModel):
    query: str

@router.post("/search")
async def search_event_handler(
    payload: SearchQuery,
    user: dict = Depends(get_current_user)
):
    try:
        query = payload.query
        model_name = "gpt-4o"
        api_key = os.getenv("OPENAI_API_KEY")

        if not api_key:
            raise HTTPException(status_code=401, detail="Missing OpenAI API key")

        user_id = user.get("USER_ID")
        if not user_id:
            raise HTTPException(status_code=400, detail="User ID is missing")

        log_user_search(user_id, query)

        results = search_events(query, model_name, api_key)
        return {"events": results}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")
