from fastapi import FastAPI
from dotenv import load_dotenv

from fastapi_backend.fast_api.routes import user_routes
from fastapi_backend.fast_api.routes import recommendation_routes
from fastapi_backend.fast_api.routes import search_routes
from fastapi_backend.fast_api.routes import chat_routes

load_dotenv()

app = FastAPI()

app.router.include_router(user_routes.router,prefix="/auth", tags=["auth"])

# Event recommendation
app.router.include_router(recommendation_routes.router, prefix="/events", tags=["recommendation"])

# Search events
app.router.include_router(search_routes.router, prefix="/events", tags=["search"])

# Chat with bot
app.router.include_router(chat_routes.router, prefix="/events", tags=["chat"])
