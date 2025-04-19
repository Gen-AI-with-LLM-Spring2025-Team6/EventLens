from pydantic import BaseModel, Field, EmailStr
from typing import List, Dict

class LoginRequest(BaseModel):
    username: str = Field(
        ..., 
        min_length=3,
        max_length=20,
        description="Username for authentication"
    )
    password: str = Field(
        ..., 
        min_length=6,
        max_length=100,
        description="User's password"
    )

class UserRegister(BaseModel):
    username: str = Field(
        ...,
        min_length=3,
        max_length=20,
        pattern="^[a-zA-Z0-9_-]+$", 
        description="The username must be unique and between 3 and 20 characters. Only letters, numbers, underscores and hyphens are allowed"
    )
    email: EmailStr = Field(
        ...,
        description="The user's email address. Must be in valid email format"
    )
    password: str = Field(
        ...,
        min_length=6,
        max_length=100,
        description="The user's password, must be at least 6 characters long"
    )
    
    interests: List[str] = Field(
        ..., min_items=1,
        description="List of user's interests. At least one must be selected."
    )
