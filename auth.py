"""
JWT utilities for Ergonode Apps Engine v2.
Ref: https://docs.ergonode.com/apps2/detailed-reference/authentication

Ergonode handshake provides shared_secret for HMAC SHA-256 JWT signing.
All requests include X-APP-TOKEN header.

Key JWT claim: app_installation_id
Sync context adds: synchronization_id
"""
import jwt
import time
import logging
from typing import Optional

logger = logging.getLogger("sellasist-app.auth")


def decode_jwt_unverified(token: str) -> dict:
    """
    Decode JWT without signature verification.
    Used to extract app_installation_id before we can look up shared_secret.
    """
    if not token:
        return {}
    try:
        return jwt.decode(
            token, algorithms=["HS256"],
            options={
                "verify_signature": False,
                "verify_exp": False,
                "verify_nbf": False,
                "verify_iat": False,
                "verify_aud": False,
            })
    except Exception as e:
        logger.warning(f"JWT decode error: {e}")
        return {}


def verify_jwt_signature(token: str, secret: str) -> Optional[dict]:
    """
    Verify JWT using shared_secret from handshake.
    Returns claims dict on success, None on failure.
    """
    if not token or not secret:
        return None
    try:
        return jwt.decode(token, secret, algorithms=["HS256"],
                          options={"verify_exp": True, "verify_nbf": True})
    except jwt.ExpiredSignatureError:
        logger.warning("JWT expired")
        return None
    except jwt.InvalidSignatureError:
        logger.warning("JWT invalid signature")
        return None
    except Exception as e:
        logger.warning(f"JWT verify error: {e}")
        return None


def create_jwt(installation_id: str, secret: str, ttl: int = 300) -> str:
    """
    Create JWT for outbound requests to Ergonode API.
    Required claims: app_installation_id, nbf, iat, exp
    """
    now = int(time.time())
    payload = {
        "app_installation_id": installation_id,
        "iat": now,
        "nbf": now,
        "exp": now + ttl,
    }
    return jwt.encode(payload, secret, algorithm="HS256")
