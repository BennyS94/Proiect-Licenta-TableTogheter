from __future__ import annotations

from pydantic import BaseModel


class RegisterRequest(BaseModel):
    email: str
    password: str
    confirm_password: str


class LoginRequest(BaseModel):
    email: str
    password: str


class AuthAccount(BaseModel):
    user_id: str
    email: str
    household_id: str
    household_display_name: str


class AuthResponse(BaseModel):
    status: str
    message: str
    session_token: str
    account: AuthAccount


class LogoutRequest(BaseModel):
    session_token: str | None = None


class MeResponse(BaseModel):
    status: str
    account: AuthAccount | None = None
