"""
Sellasist REST API client.
Async HTTP client with rate limiting and retry logic.
"""
import httpx
import asyncio
import logging
from typing import Optional, Tuple

logger = logging.getLogger("sellasist-app.client")

RATE_LIMIT_DELAY = 0.25   # 250ms between requests (Sellasist rate limit)
MAX_RETRIES = 3
RETRY_DELAY = 2.0
REQUEST_TIMEOUT = 30.0


class SellasistClient:
    def __init__(self, api_key: str, shop_domain: str):
        self.api_key = api_key
        if shop_domain.startswith("http"):
            self.base_url = shop_domain.rstrip("/")
        else:
            clean = shop_domain.strip().rstrip("/").replace(".sellasist.pl", "")
            self.base_url = f"https://{clean}.sellasist.pl/api/v1"
        self.headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "apiKey": self.api_key,
        }
        logger.info(f"[CLIENT] Init: base_url={self.base_url}")

    async def _request(self, method: str, path: str,
                       json_data=None, params=None):
        """Execute HTTP request with retry and rate limiting."""
        url = f"{self.base_url}{path}"
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                await asyncio.sleep(RATE_LIMIT_DELAY)
                async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
                    resp = await client.request(
                        method, url, headers=self.headers,
                        json=json_data, params=params)

                    if resp.status_code == 429:
                        wait = RETRY_DELAY * attempt
                        logger.warning(f"[CLIENT] 429 rate limit, wait {wait}s")
                        await asyncio.sleep(wait)
                        continue

                    if resp.status_code >= 500:
                        logger.warning(f"[CLIENT] {resp.status_code} server error")
                        await asyncio.sleep(RETRY_DELAY * attempt)
                        continue

                    resp.raise_for_status()
                    return {} if resp.status_code == 204 else resp.json()

            except httpx.TimeoutException:
                logger.warning(f"[CLIENT] Timeout attempt {attempt}/{MAX_RETRIES}")
                await asyncio.sleep(RETRY_DELAY * attempt)
            except httpx.HTTPStatusError as e:
                logger.error(f"[CLIENT] HTTP error: {e.response.status_code}")
                return {"error": str(e), "status": e.response.status_code}
            except Exception as e:
                if attempt == MAX_RETRIES:
                    logger.error(f"[CLIENT] Final error: {e}")
                    return {"error": str(e)}
                await asyncio.sleep(RETRY_DELAY)

        return {"error": "Max retries exceeded"}

    async def validate_connection(self) -> Tuple[bool, str]:
        """Test API connection. Returns (success, error_message)."""
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(
                    f"{self.base_url}/products",
                    headers=self.headers, params={"limit": 1})
                if resp.status_code == 200:
                    return True, ""
                elif resp.status_code == 401:
                    return False, "Nieprawidłowy klucz API"
                elif resp.status_code == 403:
                    return False, "Brak dostępu"
                else:
                    return False, f"HTTP {resp.status_code}"
        except httpx.TimeoutException:
            return False, "Timeout - sprawdź domenę"
        except httpx.ConnectError:
            return False, "Nie można połączyć - sprawdź domenę"
        except Exception as e:
            return False, str(e)

    # ------------------------------------------------------------------
    # Products
    # ------------------------------------------------------------------
    async def find_product_by_symbol(self, symbol: str):
        """Find product by symbol (SKU)."""
        result = await self._request("GET", "/products",
                                     params={"symbol": symbol, "limit": 1})
        if isinstance(result, list) and result:
            return result[0]
        if isinstance(result, dict) and result.get("items"):
            return result["items"][0]
        return None

    async def create_product(self, data: dict):
        """Create new product."""
        return await self._request("POST", "/products", json_data=data)

    async def update_product(self, product_id: int, data: dict):
        """Update existing product."""
        return await self._request("PUT", f"/products/{product_id}",
                                   json_data=data)

    async def delete_product(self, product_id: int):
        """Delete product."""
        return await self._request("DELETE", f"/products/{product_id}")

    # ------------------------------------------------------------------
    # Categories
    # ------------------------------------------------------------------
    async def find_category_by_name(self, name: str):
        """Find category by exact name match."""
        result = await self._request("GET", "/categories")
        if isinstance(result, list):
            for cat in result:
                if cat.get("name", "").lower() == name.lower():
                    return cat
        return None

    async def create_category(self, data: dict):
        """Create new category."""
        return await self._request("POST", "/categories", json_data=data)

    async def update_category(self, category_id: int, data: dict):
        """Update existing category."""
        return await self._request("PUT", f"/categories/{category_id}",
                                   json_data=data)

    # ------------------------------------------------------------------
    # Custom fields
    # ------------------------------------------------------------------
    async def get_product_custom_fields(self):
        """Get available custom fields for products."""
        result = await self._request("GET", "/products/custom-fields")
        if isinstance(result, list):
            return result
        if isinstance(result, dict) and "items" in result:
            return result["items"]
        return []
