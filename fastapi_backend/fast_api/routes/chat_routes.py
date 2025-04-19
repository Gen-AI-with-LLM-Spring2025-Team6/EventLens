# fastapi_backend/fast_api/routes/chat_routes.py
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from typing import List, Dict
from fastapi_backend.fast_api.services.chat_service import get_chatbot_response

router = APIRouter()

class ChatPayload(BaseModel):
    message: str
    history: List[Dict[str, str]] = []

@router.post("/chat")
async def event_chat(payload: ChatPayload, request: Request):
    try:
        message = payload.message
        history = payload.history or []

        response = get_chatbot_response(message, history)
        return {"response": response}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
