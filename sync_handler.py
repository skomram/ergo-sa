"""
Sync logic v4.3.0 - bidirectional sync.

Export (Ergonode -> Sellasist):
  Ergonode sends events with attribute data -> we push to Sellasist API.

Import (Sellasist -> Ergonode):
  Ergonode sends events (product SKU) -> we read from Sellasist API
  -> we write to Ergonode via GraphQL mutations.

Mapping is always Ergonode attr code <-> Sellasist field name.
For import the mapping is reversed: Sellasist field -> Ergonode attr code.
"""
import logging
from sellasist_client import SellasistClient
from ergonode_client import ErgonodeClient

logger = logging.getLogger("sellasist-app.sync")

FLOAT_FIELDS = {"price", "price_promo", "price_buy", "weight", "volume"}
INT_FIELDS = {"quantity", "category_id", "active"}

# Sellasist field -> Ergonode GraphQL mutation type
# Used for import to determine which mutation to call
SELLASIST_TO_ERGONODE_TYPE = {
    "name": "Text",
    "description": "Textarea",
    "price": "Numeric",
    "price_promo": "Numeric",
    "price_buy": "Numeric",
    "quantity": "Numeric",
    "ean": "Text",
    "symbol": "Text",
    "active": "Numeric",
    "weight": "Numeric",
    "volume": "Numeric",
    "category_id": "Numeric",
    "manufacturer": "Text",
    "catalog_number": "Text",
    "location": "Text",
    "pictures": "Text",
    "cf_1": "Text",
    "cf_2": "Text",
    "cf_3": "Text",
    "cf_4": "Text",
    "cf_5": "Text",
    "cf_6": "Text",
}


class SyncHandler:
    def __init__(self, sellasist_config, mapping_config, sync_config,
                 ergonode_api_url, shared_secret, installation_id):
        self.sellasist = SellasistClient(
            api_key=sellasist_config.get("api_key", ""),
            shop_domain=sellasist_config.get("shop_domain", ""))

        self.ergonode_api_url = ergonode_api_url
        self.shared_secret = shared_secret
        self.installation_id = installation_id

        self.sync_direction = sync_config.get(
            "syncDirection", "ergonode_to_sellasist")
        self.default_language = sync_config.get("defaultLanguage", "pl_PL")
        self.sync_mode = sync_config.get("syncMode", "create_and_update")
        self.sku_as_symbol = sync_config.get("skuAsSymbol", "yes") == "yes"
        self.sync_categories = (
            sync_config.get("syncCategories", "yes") == "yes")

        # Build mapping: ergonode_attr -> sellasist_field
        self.attribute_map = self._build_map(
            mapping_config.get("attributeMapping", []))

        # For import: reverse mapping sellasist_field -> ergonode_attr
        self.reverse_map = {v: k for k, v in self.attribute_map.items()}

        # Ergonode GraphQL client (for import direction)
        ergonode_api_key = sellasist_config.get("ergonode_api_key", "")
        self.ergonode = None
        if ergonode_api_key and self.sync_direction == "sellasist_to_ergonode":
            self.ergonode = ErgonodeClient(
                api_url=ergonode_api_url,
                api_key=ergonode_api_key)

        logger.info(
            f"[SYNC] direction={self.sync_direction} "
            f"mode={self.sync_mode} lang={self.default_language} "
            f"attrs={len(self.attribute_map)} sku={self.sku_as_symbol} "
            f"cats={self.sync_categories} "
            f"ergonode_client={'yes' if self.ergonode else 'no'}")

    def _build_map(self, raw: list) -> dict:
        m = {}
        skipped = 0
        for item in raw:
            erg = (item.get("ergonode", "") or "").strip()
            sel = (item.get("app", "") or "").strip()
            if not erg or not sel:
                continue
            if sel == "__skip__":
                skipped += 1
                continue
            m[erg] = sel
        if skipped:
            logger.info(f"[SYNC] Skipped {skipped} __skip__ mappings")
        logger.info(f"[SYNC] Active mappings: {m}")
        return m

    # =====================================================================
    # EXPORT: Ergonode -> Sellasist
    # =====================================================================
    async def handle_product_created(self, sku, customs, events):
        if self.sync_direction == "sellasist_to_ergonode":
            return await self._import_product(sku, customs)

        if self.sync_mode == "update_only":
            logger.info(f"[SYNC] Skip product_created {sku} - update_only")
            return None

        sid = _gc(customs, "sellasist_id")
        if not sid:
            ex = await self.sellasist.find_product_by_symbol(sku)
            if ex:
                sid = ex.get("id")
                logger.info(f"[SYNC] Found existing {sku} -> {sid}")

        data = self._extract_export(sku, events)
        logger.info(f"[SYNC] product_created {sku}: data={data}")

        if sid:
            if self.sync_mode != "create_only":
                res = await self.sellasist.update_product(int(sid), data)
                logger.info(f"[SYNC] Updated {sku}/{sid}: {res}")
        else:
            res = await self.sellasist.create_product(data)
            logger.info(f"[SYNC] Created {sku}: {res}")
            if isinstance(res, dict) and res.get("id"):
                sid = res["id"]

        return {"resource_customs": {"sellasist_id": sid}} if sid else None

    async def handle_product_updated(self, sku, customs, events):
        if self.sync_direction == "sellasist_to_ergonode":
            return await self._import_product(sku, customs)

        if self.sync_mode == "create_only":
            logger.info(f"[SYNC] Skip product_updated {sku} - create_only")
            return None

        sid = _gc(customs, "sellasist_id")
        if not sid:
            ex = await self.sellasist.find_product_by_symbol(sku)
            if ex:
                sid = ex.get("id")

        if not sid:
            if self.sync_mode == "create_and_update":
                return await self.handle_product_created(
                    sku, customs, events)
            logger.info(f"[SYNC] Product {sku} not found, skip update")
            return None

        data = self._extract_export(sku, events)
        logger.info(f"[SYNC] product_updated {sku}/{sid}: data={data}")
        await self.sellasist.update_product(int(sid), data)
        return {"resource_customs": {"sellasist_id": sid}}

    async def handle_product_deleted(self, sku, customs):
        if self.sync_direction == "sellasist_to_ergonode":
            logger.info(f"[SYNC] Import: skip product_deleted {sku}")
            return None
        sid = _gc(customs, "sellasist_id")
        if sid:
            logger.info(f"[SYNC] Deactivating {sku}/{sid}")
            await self.sellasist.update_product(int(sid), {"active": 0})

    # -- Categories (export only for now) --
    async def handle_category_created(self, code, customs, events):
        if self.sync_direction == "sellasist_to_ergonode":
            logger.info(f"[SYNC] Import: skip category {code}")
            return None
        if not self.sync_categories:
            logger.info(f"[SYNC] Skip category {code} - cats disabled")
            return None

        sid = _gc(customs, "sellasist_category_id")
        name = _etv(events, self.default_language) or code

        if sid:
            await self.sellasist.update_category(int(sid), {"name": name})
        else:
            ex = await self.sellasist.find_category_by_name(name)
            if ex:
                sid = ex.get("id")
            else:
                res = await self.sellasist.create_category({"name": name})
                if isinstance(res, dict) and res.get("id"):
                    sid = res["id"]

        return {
            "resource_customs": {"sellasist_category_id": sid}
        } if sid else None

    async def handle_category_updated(self, code, customs, events):
        return await self.handle_category_created(code, customs, events)

    async def handle_category_deleted(self, code, customs):
        if self.sync_direction == "sellasist_to_ergonode":
            return None
        if not self.sync_categories:
            return None
        sid = _gc(customs, "sellasist_category_id")
        if sid:
            await self.sellasist.delete_category(int(sid))
        return None

    # =====================================================================
    # IMPORT: Sellasist -> Ergonode
    # =====================================================================
    async def _import_product(self, sku: str, customs) -> dict:
        """
        Read product data from Sellasist, write to Ergonode via GraphQL.

        Flow:
        1. Find product in Sellasist by SKU (symbol)
        2. Read Sellasist fields that are in the reverse mapping
        3. Build GraphQL mutations to update Ergonode attributes
        4. Execute batch mutation
        """
        if not self.ergonode:
            logger.error(
                f"[SYNC] Import {sku}: no Ergonode client - "
                f"missing ergonode_api_key in configuration")
            return None

        logger.info(f"[SYNC] IMPORT product {sku}")

        # 1. Find product in Sellasist
        product = await self.sellasist.get_product_full(sku)
        if not product:
            logger.warning(f"[SYNC] Import {sku}: not found in Sellasist")
            return None

        sellasist_id = product.get("id")
        logger.info(
            f"[SYNC] Import {sku}: found in Sellasist id={sellasist_id}, "
            f"keys={list(product.keys())}")

        # 2. Extract mapped values from Sellasist product
        updates = self._extract_import(product)

        if not updates:
            logger.info(
                f"[SYNC] Import {sku}: no mapped attributes to update")
            return {
                "resource_customs": {"sellasist_id": sellasist_id}
            } if sellasist_id else None

        # 3. Execute batch mutation to Ergonode
        logger.info(
            f"[SYNC] Import {sku}: writing {len(updates)} attributes "
            f"to Ergonode: "
            f"{[(u['attribute_code'], u['value']) for u in updates]}")

        result = await self.ergonode.update_product_attributes_batch(
            sku=sku,
            updates=updates,
            language=self.default_language)

        if "errors" in result:
            logger.error(
                f"[SYNC] Import {sku}: GraphQL errors: {result['errors']}")
        else:
            logger.info(f"[SYNC] Import {sku}: Ergonode updated OK")

        return {
            "resource_customs": {"sellasist_id": sellasist_id}
        } if sellasist_id else None

    def _extract_import(self, sellasist_product: dict) -> list:
        """
        Extract values from Sellasist product using reverse mapping.
        Returns list of updates for Ergonode batch mutation.

        reverse_map: {sellasist_field: ergonode_attr_code}
        """
        updates = []

        for sellasist_field, ergonode_attr in self.reverse_map.items():
            # Get value from Sellasist product
            value = self._get_sellasist_value(
                sellasist_product, sellasist_field)

            if value is None:
                continue

            # Determine mutation type from Sellasist field
            mutation_type = SELLASIST_TO_ERGONODE_TYPE.get(
                sellasist_field, "Text")

            updates.append({
                "attribute_code": ergonode_attr,
                "value": str(value),
                "mutation_type": mutation_type,
            })

        return updates

    def _get_sellasist_value(self, product: dict,
                              field: str):
        """
        Extract a field value from Sellasist product response.
        Handles nested structures (additional_fields, pictures).
        """
        # Direct field
        if field in product and product[field] is not None:
            return product[field]

        # Custom fields: cf_1 -> additional_fields.1
        if field.startswith("cf_"):
            cf_num = field[3:]
            af = product.get("additional_fields") or {}
            if isinstance(af, dict):
                return af.get(cf_num) or af.get(field)
            if isinstance(af, list):
                for item in af:
                    if isinstance(item, dict):
                        if item.get("id") == cf_num:
                            return item.get("value")

        # Pictures
        if field == "pictures":
            pics = product.get("pictures") or product.get("images") or []
            if pics:
                urls = []
                for p in pics:
                    if isinstance(p, str):
                        urls.append(p)
                    elif isinstance(p, dict) and p.get("url"):
                        urls.append(p["url"])
                return urls[0] if urls else None

        return None

    # =====================================================================
    # EXPORT helpers
    # =====================================================================
    def _extract_export(self, sku: str, events: list) -> dict:
        """Extract data from Ergonode events for Sellasist API."""
        data = {}
        imgs = []

        if self.sku_as_symbol:
            data["symbol"] = sku

        for ev in events:
            ac = ev.get("attribute_code", "")
            if ac not in self.attribute_map:
                continue
            sf = self.attribute_map[ac]
            val = _gtv(ev, self.default_language)
            if val is None:
                continue
            try:
                if sf in FLOAT_FIELDS:
                    data[sf] = float(val)
                elif sf in INT_FIELDS:
                    data[sf] = int(float(val))
                elif sf == "pictures":
                    if isinstance(val, list):
                        imgs.extend(val)
                    else:
                        imgs.append(val)
                elif sf.startswith("cf_"):
                    data.setdefault(
                        "additional_fields", {})[sf[3:]] = str(val)
                else:
                    data[sf] = str(val)
            except (ValueError, TypeError) as e:
                logger.warning(
                    f"[SYNC] Convert error {ac}={val}->{sf}: {e}")

        if "name" not in data:
            data["name"] = sku

        if imgs:
            fmt = []
            for i in imgs:
                if isinstance(i, dict) and "url" in i:
                    fmt.append({"url": i["url"]})
                elif isinstance(i, str) and i.startswith("http"):
                    fmt.append({"url": i})
            if fmt:
                data["pictures"] = fmt

        return data


# =====================================================================
# Helpers
# =====================================================================
def _gc(customs, key):
    """Get value from resource_customs."""
    return (customs.get(key)
            if customs and isinstance(customs, dict) else None)


def _gtv(event, lang):
    """Get translated value from event."""
    for t in event.get("translations", []):
        if t.get("language") == lang:
            return t.get("value")
    tr = event.get("translations", [])
    return (tr[0].get("value") if tr
            else event.get("value") or event.get("data"))


def _etv(events, lang):
    """Get first translated value from list of events."""
    for ev in events:
        val = _gtv(ev, lang)
        if val:
            return val
    return None
