from fastapi import APIRouter, HTTPException, Depends, Request
from pydantic import BaseModel
from fastapi_backend.fast_api.services.chat_service import run_conversational_graph
from fastapi_backend.fast_api.services.auth_service import get_current_user

router = APIRouter()

class ChatPayload(BaseModel):
    message: str
    history: list

@router.post("/chat")
async def event_chat_handler(
    payload: ChatPayload,
    request: Request,
    user=Depends(get_current_user)
):
    try:
        message = payload.message
        history = payload.history

        if not message:
            raise HTTPException(status_code=400, detail="Message is required")

        response = run_conversational_graph(message, history)
        return {"response": response}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Chat processing error: {str(e)}")
