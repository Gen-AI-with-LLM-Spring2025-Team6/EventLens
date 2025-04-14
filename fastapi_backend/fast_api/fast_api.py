from fastapi import FastAPI
from dotenv import load_dotenv

from fastapi_backend.fast_api.routes import user_routes


load_dotenv()

app = FastAPI()

app.router.include_router(user_routes.router,prefix="/auth", tags=["auth"])