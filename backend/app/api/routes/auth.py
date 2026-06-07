from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Header, HTTPException

from backend.app.db.auth_repository import (
    create_session_for_account,
    create_user_with_household,
    get_session_context,
    revoke_user_session,
    verify_user_password,
)
from backend.app.schemas.auth import (
    AuthResponse,
    LoginRequest,
    LogoutRequest,
    MeResponse,
    RegisterRequest,
)


router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/register", response_model=AuthResponse)
def register_account(payload: RegisterRequest) -> dict[str, Any]:
    email = _normalize_email(payload.email)
    password = str(payload.password or "")
    confirm_password = str(payload.confirm_password or "")

    if not email or "@" not in email:
        raise HTTPException(status_code=400, detail="email_invalid")
    if len(password) < 6:
        raise HTTPException(status_code=400, detail="password_min_6")
    if password != confirm_password:
        raise HTTPException(status_code=400, detail="passwords_do_not_match")

    try:
        account = create_user_with_household(email, password)
        session = create_session_for_account(account)
    except ValueError as exc:
        detail = str(exc)
        status_code = 409 if detail == "email_already_exists" else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc

    return {
        "status": "ok",
        "message": "Account created",
        "session_token": session["session_token"],
        "account": account,
    }


@router.post("/login", response_model=AuthResponse)
def login_account(payload: LoginRequest) -> dict[str, Any]:
    account = verify_user_password(payload.email, payload.password)
    if account is None:
        raise HTTPException(status_code=401, detail="invalid_credentials")
    session = create_session_for_account(account)
    return {
        "status": "ok",
        "message": "Logged in",
        "session_token": session["session_token"],
        "account": account,
    }


@router.post("/logout")
def logout_account(payload: LogoutRequest | None = None) -> dict[str, Any]:
    token = str(payload.session_token or "").strip() if payload else ""
    if token:
        revoke_user_session(token)
    return {
        "status": "ok",
        "message": "Logged out",
    }


@router.get("/me", response_model=MeResponse)
def get_current_account(authorization: str | None = Header(default=None)) -> dict[str, Any]:
    token = _bearer_token(authorization)
    if not token:
        raise HTTPException(status_code=401, detail="unauthenticated")
    context = get_session_context(token)
    if context is None:
        raise HTTPException(status_code=401, detail="unauthenticated")
    return {
        "status": "ok",
        "account": context["account"],
    }


def _normalize_email(email: str) -> str:
    return str(email or "").strip().lower()


def _bearer_token(authorization: str | None) -> str:
    value = str(authorization or "").strip()
    prefix = "bearer "
    if value.lower().startswith(prefix):
        return value[len(prefix) :].strip()
    return ""
