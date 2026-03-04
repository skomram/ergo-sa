"""
Ergonode GraphQL API client for import (Sellasist -> Ergonode).
Ref: https://docs.ergonode.com/graphql/

Authentication: X-API-KEY header (not Apps Engine JWT).
GraphQL endpoint: {api_url}/api/graphql/
Rate limit: 500 req/min, max 6 concurrent connections.

Mutation naming convention:
  productAddAttributeValueTranslationsText
  productAddAttributeValueTranslationsTextarea
  productAddAttributeValueTranslationsNumeric
  productAddAttributeValueTranslationsSelect
  productAddAttributeValueTranslationsMultiSelect
  productAddAttributeValueTranslationsDate
  productAddAttributeValueTranslationsImage
  productAddAttributeValueTranslationsPrice
  productAddAttributeValueTranslationsUnit
"""
import httpx
import asyncio
import logging
from typing import Optional

logger = logging.getLogger("sellasist-app.ergonode")

TIMEOUT = 30.0
MAX_RETRIES = 3
RETRY_DELAY = 2.0
RATE_LIMIT_DELAY = 0.15  # ~400 req/min to stay under 500 limit


class ErgonodeClient:
    """Client for Ergonode GraphQL API using X-API-KEY auth."""

    def __init__(self, api_url: str, api_key: str):
        self.api_url = api_url.rstrip("/")
        self.graphql_url = f"{self.api_url}/api/graphql/"
        self.api_key = api_key
        self.headers = {
            "Content-Type": "application/json",
            "X-API-KEY": self.api_key,
        }
        logger.info(f"[ERGONODE] graphql_url={self.graphql_url}")

    async def _graphql_request(self, query: str,
                                variables: Optional[dict] = None) -> dict:
        """Execute GraphQL request with retry and rate limiting."""
        payload = {"query": query}
        if variables:
            payload["variables"] = variables

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                await asyncio.sleep(RATE_LIMIT_DELAY)
                async with httpx.AsyncClient(timeout=TIMEOUT) as client:
                    resp = await client.post(
                        self.graphql_url, json=payload,
                        headers=self.headers)

                    if resp.status_code == 429:
                        wait = RETRY_DELAY * attempt * 2
                        logger.warning(
                            f"[ERGONODE] Rate limited, waiting {wait}s "
                            f"(attempt {attempt})")
                        await asyncio.sleep(wait)
                        continue

                    if resp.status_code == 401:
                        logger.error(
                            "[ERGONODE] 401 Unauthorized - check API key "
                            "and write permissions")
                        return {"errors": [{"message":
                            "Brak autoryzacji - sprawdź klucz API Ergonode "
                            "i uprawnienia zapisu"}]}

                    if resp.status_code >= 500:
                        logger.warning(
                            f"[ERGONODE] Server error {resp.status_code}, "
                            f"attempt {attempt}")
                        await asyncio.sleep(RETRY_DELAY * attempt)
                        continue

                    result = resp.json()

                    if "errors" in result:
                        errors = result["errors"]
                        retriable = any(
                            e.get("extensions", {}).get("retriable", False)
                            for e in errors)
                        if retriable and attempt < MAX_RETRIES:
                            logger.warning(
                                f"[ERGONODE] Retriable errors: {errors}")
                            await asyncio.sleep(RETRY_DELAY * attempt)
                            continue
                        logger.error(f"[ERGONODE] GraphQL errors: {errors}")

                    return result

            except httpx.TimeoutException:
                logger.warning(
                    f"[ERGONODE] Timeout attempt {attempt}/{MAX_RETRIES}")
                await asyncio.sleep(RETRY_DELAY * attempt)
            except Exception as e:
                logger.error(f"[ERGONODE] Request error: {e}")
                if attempt == MAX_RETRIES:
                    return {"errors": [{"message": str(e)}]}
                await asyncio.sleep(RETRY_DELAY)

        return {"errors": [{"message": "Max retries exceeded"}]}

    async def test_connection(self) -> tuple:
        """Test connection and write access."""
        result = await self._graphql_request("{ languageList { code } }")
        if "errors" in result:
            msg = result["errors"][0].get("message", "Unknown error")
            return False, msg
        if "data" in result and result["data"].get("languageList"):
            return True, ""
        return False, "Unexpected response"

    async def update_product_attribute(
            self, sku: str, attribute_code: str, value: str,
            language: str, mutation_type: str = "Text") -> dict:
        """
        Update a single attribute value on a product.

        mutation_type: Text, Textarea, Numeric, Select, MultiSelect,
                       Date, Image, Price, Unit
        """
        mutation_name = (
            f"productAddAttributeValueTranslations{mutation_type}")
        # Escape special chars in value for GraphQL string
        safe_value = (str(value)
                      .replace("\\", "\\\\")
                      .replace('"', '\\"')
                      .replace("\n", "\\n"))

        query = f"""
        mutation {{
            update: {mutation_name}(
                input: {{
                    sku: "{sku}"
                    attributeCode: "{attribute_code}"
                    translations: [{{
                        value: "{safe_value}"
                        language: "{language}"
                    }}]
                }}
            ) {{
                __typename
            }}
        }}
        """

        logger.info(
            f"[ERGONODE] {sku}.{attribute_code} = {safe_value} "
            f"(lang={language}, type={mutation_type})")

        result = await self._graphql_request(query)

        if "errors" in result:
            logger.error(
                f"[ERGONODE] Failed {sku}.{attribute_code}: "
                f"{result['errors']}")
        else:
            logger.info(f"[ERGONODE] Updated {sku}.{attribute_code} OK")

        return result

    async def update_product_attributes_batch(
            self, sku: str, updates: list, language: str) -> dict:
        """
        Batch update multiple attributes on a product (max 50).

        updates: [{"attribute_code": str, "value": str,
                   "mutation_type": str}, ...]
        """
        if not updates:
            return {"data": {}}

        mutations = []
        for i, upd in enumerate(updates[:50]):
            attr_code = upd["attribute_code"]
            safe_value = (str(upd["value"])
                          .replace("\\", "\\\\")
                          .replace('"', '\\"')
                          .replace("\n", "\\n"))
            mut_type = upd.get("mutation_type", "Text")
            mut_name = (
                f"productAddAttributeValueTranslations{mut_type}")
            alias = f"attr_{i}"

            mutations.append(f"""
            {alias}: {mut_name}(
                input: {{
                    sku: "{sku}"
                    attributeCode: "{attr_code}"
                    translations: [{{
                        value: "{safe_value}"
                        language: "{language}"
                    }}]
                }}
            ) {{ __typename }}""")

        query = "mutation {\n" + "\n".join(mutations) + "\n}"

        logger.info(
            f"[ERGONODE] Batch update {sku}: {len(mutations)} attributes "
            f"({[u['attribute_code'] for u in updates[:50]]})")

        result = await self._graphql_request(query)

        if "errors" in result:
            logger.error(
                f"[ERGONODE] Batch {sku} errors: {result['errors']}")
        else:
            logger.info(f"[ERGONODE] Batch {sku} OK")

        return result
