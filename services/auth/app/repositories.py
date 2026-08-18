"""Database access. No business rules, no HTTP concerns."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from common.auth.jwt import Role
from common.db.base import utcnow
from app.models import RefreshToken, User


class UserRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_id(self, user_id: UUID) -> User | None:
        return await self._session.get(User, user_id)

    async def get_by_email(self, email: str) -> User | None:
        result = await self._session.execute(
            select(User).where(func.lower(User.email) == email.lower())
        )
        return result.scalar_one_or_none()

    async def email_exists(self, email: str) -> bool:
        result = await self._session.execute(
            select(User.id).where(func.lower(User.email) == email.lower())
        )
        return result.scalar_one_or_none() is not None

    async def create(
        self,
        *,
        email: str,
        password_hash: str,
        full_name: str,
        role: Role = Role.CUSTOMER,
    ) -> User:
        user = User(
            email=email.lower(),
            password_hash=password_hash,
            full_name=full_name,
            role=role.value,
            is_active=True,
        )
        self._session.add(user)
        await self._session.flush()
        await self._session.refresh(user)
        return user

    async def list_users(
        self,
        *,
        offset: int,
        limit: int,
        role: Role | None = None,
        is_active: bool | None = None,
        search: str | None = None,
    ) -> tuple[list[User], int]:
        conditions = []
        if role is not None:
            conditions.append(User.role == role.value)
        if is_active is not None:
            conditions.append(User.is_active.is_(is_active))
        if search:
            pattern = f"%{search.lower()}%"
            conditions.append(
                func.lower(User.email).like(pattern) | func.lower(User.full_name).like(pattern)
            )

        base = select(User)
        count_stmt = select(func.count()).select_from(User)
        for condition in conditions:
            base = base.where(condition)
            count_stmt = count_stmt.where(condition)

        total = int((await self._session.execute(count_stmt)).scalar_one())
        rows = (
            (
                await self._session.execute(
                    base.order_by(User.created_at.desc()).offset(offset).limit(limit)
                )
            )
            .scalars()
            .all()
        )
        return list(rows), total

    async def set_role(self, user: User, role: Role) -> User:
        user.role = role.value
        await self._session.flush()
        return user

    async def set_active(self, user: User, is_active: bool) -> User:
        user.is_active = is_active
        await self._session.flush()
        return user

    async def touch_last_login(self, user: User) -> None:
        user.last_login_at = utcnow()
        await self._session.flush()


class RefreshTokenRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def store(
        self, *, token_id: UUID, user_id: UUID, token_hash: str, expires_at: datetime
    ) -> RefreshToken:
        token = RefreshToken(
            id=token_id, user_id=user_id, token_hash=token_hash, expires_at=expires_at
        )
        self._session.add(token)
        await self._session.flush()
        return token

    async def get_by_hash(self, token_hash: str) -> RefreshToken | None:
        result = await self._session.execute(
            select(RefreshToken).where(RefreshToken.token_hash == token_hash)
        )
        return result.scalar_one_or_none()

    async def revoke(self, token: RefreshToken) -> None:
        token.revoked_at = utcnow()
        await self._session.flush()

    async def revoke_all_for_user(self, user_id: UUID) -> int:
        """Used when an admin deactivates an account - the access token still
        works until it expires, but no new one can be minted."""
        result = await self._session.execute(
            update(RefreshToken)
            .where(RefreshToken.user_id == user_id, RefreshToken.revoked_at.is_(None))
            .values(revoked_at=utcnow())
        )
        await self._session.flush()
        return int(result.rowcount or 0)
