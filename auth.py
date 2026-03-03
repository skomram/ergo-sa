"""
JWT utilities for Ergonode Apps Engine authentication.
Ref: https://docs.ergonode.com/apps2/detailed-reference/authentication

Ergonode uses HMAC SHA-256 JWT tokens with shared_secret received
during handshake. All requests include X-APP-TOKEN header.

Claims in sync context: ergonode_api_url, app_id, synchronization_id
"""
import jwt
import time
import logging
from typing import Optional

logger = logging.getLogger("sellasist-app.auth")


def decode_jwt_unverified(token: str) -> dict:
    """Decode JWT without signature verification (for extracting claims)."""
    if not token:
        return {}
    try:
        claims = jwt.decode(
            token, algorithms=["HS256"],
            options={"verify_signature": False, "verify_exp": False,
                     "verify_nbf": False, "verify_iat": False,
                     "verify_aud": False})
        return claims
    except Exception as e:
        logger.warning(f"JWT decode error: {e}")
        return {}


def verify_jwt_signature(token: str, secret: str) -> Optional[dict]:
    """Verify JWT signature using shared_secret from handshake."""
    if not token or not secret:
        return None
    try:
        claims = jwt.decode(token, secret, algorithms=["HS256"],
                            options={"verify_exp": True, "verify_nbf": True})
        return claims
    except jwt.ExpiredSignatureError:
        logger.warning("JWT expired")
        return None
    except jwt.InvalidSignatureError:
        logger.warning("JWT invalid signature")
        return None
    except Exception as e:
        logger.warning(f"JWT verification error: {e}")
        return None


def create_jwt(installation_id: str, secret: str, ttl: int = 300) -> str:
    """Create JWT for outbound requests to Ergonode API (if needed)."""
    now = int(time.time())
    payload = {"app_installation_id": installation_id,
               "iat": now, "nbf": now, "exp": now + ttl}
    return jwt.encode(payload, secret, algorithm="HS256")
