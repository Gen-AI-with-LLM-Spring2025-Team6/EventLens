from fastapi import APIRouter, HTTPException, status
from fastapi_backend.fast_api.schema.request_schema import LoginRequest, UserRegister
from fastapi_backend.fast_api.services.auth_service import password_hashing, create_jwt_token
from fastapi_backend.fast_api.services.user_service import fetch_user, insert_user, fetch_user_by_email

router = APIRouter()

@router.post("/login")
def login(request: LoginRequest):
    """
    Authenticate user and return a JWT token.
    """
    if not request.username or not request.password:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username and password are required",
        )

    user = fetch_user(request.username)

    if user is None or user.empty:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        stored_password = user.iloc[0]["USERPASSWORD"]
        username = user.iloc[0]["USERNAME"]
        hashed_input_password = password_hashing(request.password)

        if stored_password == hashed_input_password:
            token, expiry_time = create_jwt_token({"username": username})
            return {
                "access_token": token,
                "token_type": "bearer",
                "expiry_time": expiry_time.isoformat(),
                "username": username,
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid username or password",
                headers={"WWW-Authenticate": "Bearer"},
            )
    except IndexError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error accessing user data",
        )
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Unexpected error during login. Please try again.",
        )


@router.post("/register", status_code=status.HTTP_201_CREATED)
async def register(request: UserRegister):
    """
    Register a new user.
    """
    username = request.username.strip()
    email = request.email.strip()
    password = request.password

    # Check if username exists
    user_by_username = fetch_user(username)
    if user_by_username is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Username already taken",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Check if email exists
    user_by_email = fetch_user_by_email(email)
    if user_by_email is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Email already registered",
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        hashed_password = password_hashing(password)
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error processing password"
        )

    try:
        insert_user(username, email, hashed_password, request.interests)
        return {
            "status": "success",
            "message": "User registered successfully",
            "username": username,
            "email": email
        }
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to register user"
        )
