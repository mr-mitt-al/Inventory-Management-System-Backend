"""Unit tests for hashing and tokens - no database or broker needed."""

from __future__ import annotations

import time
from uuid import uuid4

import pytest

from common.auth.jwt import (
    Role,
    TokenType,
    create_access_token,
    create_refresh_token,
    decode_token,
)
from common.errors import UnauthorizedError
from app.security import (
    PasswordTooLongError,
    hash_password,
    hash_refresh_token,
    refresh_token_matches,
    verify_password,
)

SECRET = "test-secret-not-used-anywhere-real"


class TestPasswordHashing:
    def test_round_trip(self) -> None:
        h = hash_password("Sup3rSecret!")
        assert h != "Sup3rSecret!"
        assert verify_password("Sup3rSecret!", h)

    def test_wrong_password_rejected(self) -> None:
        h = hash_password("Sup3rSecret!")
        assert not verify_password("sup3rsecret!", h)

    def test_same_password_gives_different_hashes(self) -> None:
        # Distinct salts: identical passwords must not produce identical hashes,
        # or a leaked table reveals which users share a password.
        assert hash_password("Same@12345") != hash_password("Same@12345")

    def test_over_72_bytes_rejected_not_truncated(self) -> None:
        # bcrypt silently ignores bytes past 72. Truncating would make two
        # different long passwords interchangeable, so reject instead.
        with pytest.raises(PasswordTooLongError):
            hash_password("a" * 73)

    def test_malformed_stored_hash_is_a_failed_login_not_a_crash(self) -> None:
        assert not verify_password("whatever", "not-a-bcrypt-hash")


class TestRefreshTokenHashing:
    def test_deterministic(self) -> None:
        assert hash_refresh_token("abc") == hash_refresh_token("abc")

    def test_matches(self) -> None:
        token = "some-long-random-refresh-token"
        assert refresh_token_matches(token, hash_refresh_token(token))
        assert not refresh_token_matches("other", hash_refresh_token(token))


class TestTokens:
    def test_access_token_round_trip(self) -> None:
        user_id = uuid4()
        token, jti, expires_at = create_access_token(
            user_id=user_id, email="a@b.com", role=Role.CUSTOMER, secret=SECRET
        )
        decoded = decode_token(token, secret=SECRET, expected_type=TokenType.ACCESS)

        assert decoded.user_id == user_id
        assert decoded.role is Role.CUSTOMER
        assert decoded.jti == jti
        assert expires_at.timestamp() > time.time()

    def test_role_travels_in_the_token(self) -> None:
        # This is what lets every other service authorize locally instead of
        # calling the auth service on every request.
        token, _, _ = create_access_token(
            user_id=uuid4(), email="admin@b.com", role=Role.ADMIN, secret=SECRET
        )
        assert decode_token(token, secret=SECRET).is_admin

    def test_wrong_secret_rejected(self) -> None:
        token, _, _ = create_access_token(
            user_id=uuid4(), email="a@b.com", role=Role.CUSTOMER, secret=SECRET
        )
        with pytest.raises(UnauthorizedError):
            decode_token(token, secret="a-different-secret")

    def test_refresh_token_rejected_where_access_expected(self) -> None:
        # Guards against a 7-day token being used as a bearer token on a normal
        # endpoint, where a 15-minute one was intended.
        token, _, _ = create_refresh_token(
            user_id=uuid4(), email="a@b.com", role=Role.CUSTOMER, secret=SECRET
        )
        with pytest.raises(UnauthorizedError):
            decode_token(token, secret=SECRET, expected_type=TokenType.ACCESS)

    def test_expired_token_rejected(self) -> None:
        token, _, _ = create_access_token(
            user_id=uuid4(),
            email="a@b.com",
            role=Role.CUSTOMER,
            secret=SECRET,
            expire_minutes=-1,  # already expired
        )
        with pytest.raises(UnauthorizedError, match="expired"):
            decode_token(token, secret=SECRET)

    def test_garbage_rejected(self) -> None:
        with pytest.raises(UnauthorizedError):
            decode_token("not.a.jwt", secret=SECRET)
