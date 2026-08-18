"""The privilege-escalation guard, asserted rather than assumed.

If someone later adds a `role` field to RegisterRequest "for convenience", this
test fails and explains why that is not a convenience.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from common.auth.jwt import Role
from app.schemas import RegisterRequest


def test_register_request_has_no_role_field() -> None:
    assert "role" not in RegisterRequest.model_fields, (
        "RegisterRequest must never accept a role - a client could then POST "
        '{"role": "admin"} and take over the system. Admins come from the '
        "startup bootstrap or an admin-only promotion endpoint."
    )


def test_role_in_body_is_ignored_not_honoured() -> None:
    body = RegisterRequest.model_validate(
        {
            "email": "attacker@example.com",
            "password": "Passw0rd!",
            "full_name": "Attacker",
            "role": "admin",  # extra field, silently dropped by pydantic
        }
    )
    assert not hasattr(body, "role")


def test_service_hardcodes_customer_role() -> None:
    """AuthService.register passes Role.CUSTOMER literally, never a parameter."""
    import inspect

    from app.services import AuthService

    source = inspect.getsource(AuthService.register)
    assert "Role.CUSTOMER" in source
    assert "role=role" not in source, "register() must not accept a caller-supplied role"


@pytest.mark.parametrize(
    "password",
    [
        "short",           # under the minimum length
        "12345678",        # digits only
        "abcdefghij",      # letters only
        "a" * 80,          # over bcrypt's 72-byte limit
    ],
)
def test_weak_passwords_rejected(password: str) -> None:
    with pytest.raises(ValidationError):
        RegisterRequest.model_validate(
            {"email": "a@b.com", "password": password, "full_name": "A"}
        )


def test_reasonable_password_accepted() -> None:
    body = RegisterRequest.model_validate(
        {"email": "a@b.com", "password": "Passw0rd!", "full_name": "  Sanket  "}
    )
    assert body.full_name == "Sanket"  # trimmed


def test_only_two_roles_exist() -> None:
    assert set(Role) == {Role.CUSTOMER, Role.ADMIN}
