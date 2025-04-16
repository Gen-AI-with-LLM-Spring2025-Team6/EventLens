from fastapi import APIRouter, HTTPException, status, Request
from fastapi.responses import JSONResponse
from fastapi_backend.fast_api.services.search_service import get_sample_events

router = APIRouter()

@router.post("/search")
async def search_events(request: Request):
    try:
        events = get_sample_events()
        return JSONResponse(status_code=200, content={"events": events})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
