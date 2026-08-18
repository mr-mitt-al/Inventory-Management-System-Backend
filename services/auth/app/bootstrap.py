"""Admin bootstrap - the answer to "how do I become admin?".

Signup hardcodes `role=customer`, so no HTTP request can ever create an admin.
The first one comes from here: on startup, if BOOTSTRAP_ADMIN_EMAIL and
BOOTSTRAP_ADMIN_PASSWORD are set and that user does not exist, create it with
role=admin. Then log in through the normal /auth/login.

Idempotent, so it is safe on every restart, and it survives
`docker compose down -v` - unlike a manual `UPDATE users SET role='admin'`,
which you would redo every time you rebuilt the volume.

Rejected alternatives:
  - seed migration with a hardcoded admin  -> commits a password hash to git
  - /setup endpoint valid while 0 users    -> race condition and an attack
                                              surface for no benefit

This is how Keycloak, Grafana and Airflow all do it.
"""

from __future__ import annotations

import logging

from common.auth.jwt import Role
from common.db.session import Database
from app.config import settings
from app.repositories import UserRepository
from app.security import hash_password

logger = logging.getLogger(__name__)


async def seed_admin(db: Database) -> None:
    email = (settings.bootstrap_admin_email or "").strip()
    password = settings.bootstrap_admin_password or ""

    if not email or not password:
        logger.info("admin bootstrap skipped (BOOTSTRAP_ADMIN_* not set)")
        return

    async with db.transaction() as session:
        users = UserRepository(session)
        existing = await users.get_by_email(email)

        if existing is not None:
            # Do not silently re-promote: if someone deliberately demoted this
            # account, startup should not undo that. Warn instead.
            if existing.role != Role.ADMIN.value:
                logger.warning(
                    "bootstrap admin email exists but is not an admin; leaving as-is",
                    extra={"email": email, "role": existing.role},
                )
            else:
                logger.info("bootstrap admin already present", extra={"email": email})
            return

        user = await users.create(
            email=email,
            password_hash=hash_password(password),
            full_name=settings.bootstrap_admin_name,
            role=Role.ADMIN,
        )
        logger.warning(
            "BOOTSTRAP ADMIN CREATED - change this password outside development",
            extra={"email": email, "user_id": str(user.id)},
        )
