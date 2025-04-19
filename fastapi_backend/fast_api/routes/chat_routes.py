from fastapi import APIRouter

router = APIRouter()

@router.post("/chat")
def chat_with_bot(prompt: str):
    # return LLM-generated response
    pass
