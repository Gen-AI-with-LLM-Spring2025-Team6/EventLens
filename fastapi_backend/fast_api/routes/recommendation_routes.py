from fastapi import APIRouter, Depends, HTTPException
from fastapi_backend.fast_api.services.auth_service import get_current_user
from fastapi_backend.fast_api.services.recommendation_service import fetch_recommended_events

router = APIRouter()

@router.get("/recommend")
def recommend_events(user: dict = Depends(get_current_user)):
    try:
        user_id = user.get("USER_ID")
        if not user_id:
            raise HTTPException(status_code=400, detail="User ID is missing.")

        events = fetch_recommended_events(user_id)
        return {"events": events}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch recommendations: {str(e)}")
