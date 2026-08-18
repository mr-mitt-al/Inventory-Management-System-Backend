from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator

from common.auth.jwt import Role
from app.config import settings


class RegisterRequest(BaseModel):
    """Note what is NOT here: `role`.

    If this model accepted a role, anyone could POST {"role": "admin"} and own
    the system. The field is absent rather than "validated" - there is no code
    path from a request body to an elevated role. Admins are created by the
    startup bootstrap or promoted by an existing admin.
    """

    email: EmailStr
    password: str = Field(min_length=8, max_length=72)
    full_name: str = Field(min_length=1, max_length=200)

    @field_validator("password")
    @classmethod
    def check_strength(cls, v: str) -> str:
        if len(v) < settings.password_min_length:
            raise ValueError(f"password must be at least {settings.password_min_length} characters")
        if len(v.encode("utf-8")) > 72:
            raise ValueError("password must be at most 72 bytes when utf-8 encoded")
        if v.isdigit() or v.isalpha():
            raise ValueError("password must mix letters with digits or symbols")
        return v

    @field_validator("full_name")
    @classmethod
    def strip_name(cls, v: str) -> str:
        cleaned = v.strip()
        if not cleaned:
            raise ValueError("full_name cannot be blank")
        return cleaned


class LoginRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=1, max_length=200)


class RefreshRequest(BaseModel):
    refresh_token: str = Field(min_length=1)


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int = Field(description="Seconds until the access token expires")


class UserResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    email: str
    full_name: str
    role: Role
    is_active: bool
    last_login_at: datetime | None
    created_at: datetime


class RoleUpdateRequest(BaseModel):
    role: Role


class StatusUpdateRequest(BaseModel):
    is_active: bool


class RegisterResponse(BaseModel):
    user: UserResponse
    message: str = "account created"
