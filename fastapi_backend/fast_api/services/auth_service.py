import os
import base64
import hmac
import hashlib
import jwt
from fastapi import HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

from fastapi_backend.fast_api.services.user_service import fetch_user

load_dotenv()

SECRET_KEY = os.getenv("SECRET_KEY")
if not SECRET_KEY:
    raise ValueError("SECRET_KEY environment variable is not set")

security = HTTPBearer()


def password_hashing(password: str) -> str:
    """Hash the password for secure storage."""
    if not isinstance(password, str):
        raise ValueError("Password must be a string")
    if not password:
        raise ValueError("Password cannot be empty")

    try:
        secret_key = base64.b64encode(SECRET_KEY.encode())
        return hmac.new(secret_key, msg=password.encode(), digestmod=hashlib.sha256).hexdigest()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error during password hashing: {str(e)}"
        )


def token_hashing(token: str) -> str:
    """Hash a token (e.g., GitHub token) for secure storage."""
    if not isinstance(token, str):
        raise ValueError("Token must be a string")
    if not token:
        raise ValueError("Token cannot be empty")

    try:
        secret_key = base64.b64encode(SECRET_KEY.encode())
        return hmac.new(secret_key, msg=token.encode(), digestmod=hashlib.sha256).hexdigest()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error during token hashing: {str(e)}"
        )


def create_jwt_token(data: dict):
    """Generate a JWT token for the user."""
    if not isinstance(data, dict):
        raise ValueError("Data must be a dictionary")
    if not data:
        raise ValueError("Data dictionary cannot be empty")

    try:
        expiry_time = datetime.now(timezone.utc) + timedelta(hours=1)
        token_payload = {"exp": expiry_time, **data}
        token = jwt.encode(token_payload, SECRET_KEY, algorithm="HS256")
        return token, expiry_time
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error creating JWT token: {str(e)}"
        )


def decode_jwt_token(token: str) -> dict:
    """Decode and validate a JWT token."""
    if not isinstance(token, str):
        raise ValueError("Token must be a string")
    if not token:
        raise ValueError("Token cannot be empty")

    try:
        return jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except jwt.InvalidTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error decoding token: {str(e)}"
        )


def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Extract the current user from a validated JWT token."""
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="No credentials provided",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = credentials.credentials
    payload = decode_jwt_token(token)
    username = payload.get("username")

    if not username:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token: missing username",
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        user = fetch_user(username)
        if user is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found",
                headers={"WWW-Authenticate": "Bearer"},
            )
        try:
            return user.to_dict(orient='records')[0]
        except (AttributeError, IndexError) as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error processing user data: {str(e)}"
            )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unexpected error retrieving user: {str(e)}"
        )
