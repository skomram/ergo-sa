"""
Sync logic Ergonode -> Sellasist v3.4.0
========================================
Obsługuje eventy synchronizacji z Ergonode Apps Engine v2:
- product_created / product_updated / product_deleted
- category_created / category_updated / category_deleted

Mapper widget zapisuje tablicę obiektów:
  [{"ergonode": "atrybut_ergonode", "app": "pole_sellasist"}, ...]

Pole __skip__ w app dictionary oznacza "nie synchronizuj".
Filtrowane przy budowie attribute_map.
"""
import logging
from sellasist_client import SellasistClient

logger = logging.getLogger("sellasist-app.sync")

# Pola Sellasist wymagajace konwersji na float
FLOAT_FIELDS = {"price", "price_promo", "price_buy", "weight", "volume"}
# Pola Sellasist wymagajace konwersji na int
INT_FIELDS = {"quantity", "category_id", "active"}


class SyncHandler:
    def __init__(self, sellasist_config, mapping_config, sync_config,
                 ergonode_api_url, shared_secret, installation_id):
        self.sellasist = SellasistClient(
            api_key=sellasist_config.get("x_sa_token", ""),
            shop_domain=sellasist_config.get("x_sa_host", ""))

        self.mapping = mapping_config
        self.sync_config = sync_config
        self.ergonode_api_url = ergonode_api_url
        self.shared_secret = shared_secret
        self.installation_id = installation_id

        # Sync settings (step 2)
        self.default_language = sync_config.get("defaultLanguage", "pl_PL")
        self.sync_mode = sync_config.get("syncMode", "create_and_update")
        self.sku_as_symbol = sync_config.get("skuAsSymbol", "yes") == "yes"
        self.sync_categories = sync_config.get("syncCategories", "yes") == "yes"

        # Build attribute map from mapper widget data
        self.attribute_map = self._build_attribute_map(
            mapping_config.get("attributeMapping", []))

        logger.info(
            f"[SYNC] Init: mode={self.sync_mode} lang={self.default_language} "
            f"mapped_attrs={len(self.attribute_map)} "
            f"sku_as_symbol={self.sku_as_symbol} "
            f"categories={self.sync_categories}")

    def _build_attribute_map(self, raw_mapping: list) -> dict:
        """
        Parse mapper widget output into {ergonode_attr: sellasist_field} dict.
        Filters out __skip__ entries.
        """
        attr_map = {}
        skipped = 0
        for item in raw_mapping:
            erg = (item.get("ergonode", "") or "").strip()
            sel = (item.get("app", "") or "").strip()
            if not erg or not sel:
                continue
            if sel == "__skip__":
                skipped += 1
                continue
            attr_map[erg] = sel

        if skipped:
            logger.info(f"[SYNC] Skipped {skipped} __skip__ mappings")
        return attr_map

    # ------------------------------------------------------------------
    # Product handlers
    # ------------------------------------------------------------------
    async def handle_product_created(self, sku, customs, events):
        """Handle product_created event."""
        if self.sync_mode == "update_only":
            logger.debug(f"[SYNC] Skip create (update_only): {sku}")
            return None

        sid = _gc(customs, "sellasist_id")

        if not sid:
            existing = await self.sellasist.find_product_by_symbol(sku)
            if existing:
                sid = existing.get("id")
                logger.info(f"[SYNC] Found existing: {sku} -> {sid}")

        data = self._extract_product_data(sku, events)

        if sid:
            if self.sync_mode != "create_only":
                await self.sellasist.update_product(int(sid), data)
                logger.info(f"[SYNC] Updated existing: {sku} ({sid})")
        else:
            res = await self.sellasist.create_product(data)
            if isinstance(res, dict) and res.get("id"):
                sid = res["id"]
                logger.info(f"[SYNC] Created: {sku} -> {sid}")
            elif isinstance(res, dict) and res.get("error"):
                logger.error(f"[SYNC] Create failed: {sku}: {res['error']}")
                return None

        return {"resource_customs": {"sellasist_id": sid}} if sid else None

    async def handle_product_updated(self, sku, customs, events):
        """Handle product_updated event."""
        if self.sync_mode == "create_only":
            logger.debug(f"[SYNC] Skip update (create_only): {sku}")
            return None

        sid = _gc(customs, "sellasist_id")

        if not sid:
            existing = await self.sellasist.find_product_by_symbol(sku)
            if existing:
                sid = existing.get("id")

        if not sid:
            if self.sync_mode == "create_and_update":
                logger.info(f"[SYNC] Not found, creating: {sku}")
                return await self.handle_product_created(sku, customs, events)
            return None

        data = self._extract_product_data(sku, events)
        await self.sellasist.update_product(int(sid), data)
        logger.info(f"[SYNC] Updated: {sku} ({sid})")

        return {"resource_customs": {"sellasist_id": sid}}

    async def handle_product_deleted(self, sku, customs):
        """Handle product_deleted - deactivate in Sellasist."""
        sid = _gc(customs, "sellasist_id")
        if sid:
            await self.sellasist.update_product(int(sid), {"active": 0})
            logger.info(f"[SYNC] Deactivated: {sku} ({sid})")

    # ------------------------------------------------------------------
    # Category handlers
    # ------------------------------------------------------------------
    async def handle_category_created(self, code, customs, events):
        """Handle category_created event."""
        if not self.sync_categories:
            return None

        sid = _gc(customs, "sellasist_category_id")
        name = _extract_translation(events, self.default_language) or code

        if sid:
            await self.sellasist.update_category(int(sid), {"name": name})
        else:
            existing = await self.sellasist.find_category_by_name(name)
            if existing:
                sid = existing.get("id")
            else:
                res = await self.sellasist.create_category({"name": name})
                if isinstance(res, dict) and res.get("id"):
                    sid = res["id"]

        return {"resource_customs": {"sellasist_category_id": sid}} if sid else None

    async def handle_category_updated(self, code, customs, events):
        """Handle category_updated event."""
        return await self.handle_category_created(code, customs, events)

    # ------------------------------------------------------------------
    # Data extraction
    # ------------------------------------------------------------------
    def _extract_product_data(self, sku: str, events: list) -> dict:
        """
        Extract Sellasist product payload from Ergonode sync events.
        Uses attribute_map to translate Ergonode attribute codes
        to Sellasist field names.
        """
        data = {}
        images = []

        if self.sku_as_symbol:
            data["symbol"] = sku

        for event in events:
            attr_code = event.get("attribute_code", "")
            if attr_code not in self.attribute_map:
                continue

            sellasist_field = self.attribute_map[attr_code]
            value = _get_translated_value(event, self.default_language)

            if value is None:
                continue

            try:
                if sellasist_field in FLOAT_FIELDS:
                    data[sellasist_field] = float(value)

                elif sellasist_field in INT_FIELDS:
                    data[sellasist_field] = int(float(value))

                elif sellasist_field == "pictures":
                    if isinstance(value, list):
                        images.extend(value)
                    else:
                        images.append(value)

                elif sellasist_field.startswith("cf_"):
                    cf_key = sellasist_field[3:]
                    data.setdefault("additional_fields", {})[cf_key] = str(value)

                else:
                    data[sellasist_field] = str(value)

            except (ValueError, TypeError) as e:
                logger.warning(
                    f"[SYNC] Type conversion error: {attr_code}={value} "
                    f"-> {sellasist_field}: {e}")

        if "name" not in data:
            data["name"] = sku

        if images:
            formatted = []
            for img in images:
                if isinstance(img, dict) and "url" in img:
                    formatted.append({"url": img["url"]})
                elif isinstance(img, str) and img.startswith("http"):
                    formatted.append({"url": img})
            if formatted:
                data["pictures"] = formatted

        return data


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------
def _gc(customs, key):
    """Get value from resource_customs safely."""
    if customs and isinstance(customs, dict):
        return customs.get(key)
    return None


def _get_translated_value(event: dict, lang: str):
    """
    Extract translated value from sync event.
    Priority: requested language -> first available -> raw value.
    """
    translations = event.get("translations", [])
    for t in translations:
        if t.get("language") == lang:
            return t.get("value")
    if translations:
        return translations[0].get("value")
    return event.get("value") or event.get("data")


def _extract_translation(events: list, lang: str):
    """Extract first available translation from a list of events."""
    for event in events:
        val = _get_translated_value(event, lang)
        if val:
            return val
    return None
