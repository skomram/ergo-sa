"""
Sync logic Ergonode -> Sellasist v4.2.0

Handles synchronization events from Ergonode Apps Engine v2.
Mapper widget output: [{"ergonode": "attr_code", "app": "sellasist_field"}, ...]
__skip__ entries are filtered out.

Config field names match manifest.json:
  Step 0: shop_domain, api_key
  Step 1: attributeMapping
  Step 2: syncDirection, defaultLanguage, syncMode, skuAsSymbol, syncCategories
"""
import logging
from sellasist_client import SellasistClient

logger = logging.getLogger("sellasist-app.sync")

FLOAT_FIELDS = {"price", "price_promo", "price_buy", "weight", "volume"}
INT_FIELDS = {"quantity", "category_id", "active"}


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
        self.sync_categories = sync_config.get(
            "syncCategories", "yes") == "yes"

        self.attribute_map = self._build_map(
            mapping_config.get("attributeMapping", []))

        logger.info(
            f"[SYNC] direction={self.sync_direction} "
            f"mode={self.sync_mode} lang={self.default_language} "
            f"attrs={len(self.attribute_map)} sku={self.sku_as_symbol} "
            f"cats={self.sync_categories}")

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

    # -- Products ----------------------------------------------------------
    async def handle_product_created(self, sku, customs, events):
        if self.sync_mode == "update_only":
            logger.info(f"[SYNC] Skipping product_created {sku} "
                        f"- mode is update_only")
            return None

        sid = _gc(customs, "sellasist_id")
        if not sid:
            ex = await self.sellasist.find_product_by_symbol(sku)
            if ex:
                sid = ex.get("id")
                logger.info(f"[SYNC] Found existing product {sku} -> {sid}")

        data = self._extract(sku, events)
        logger.info(f"[SYNC] product_created {sku}: sending data={data}")

        if sid:
            if self.sync_mode != "create_only":
                res = await self.sellasist.update_product(int(sid), data)
                logger.info(f"[SYNC] Updated product {sku}/{sid}: {res}")
        else:
            res = await self.sellasist.create_product(data)
            logger.info(f"[SYNC] Created product {sku}: {res}")
            if isinstance(res, dict) and res.get("id"):
                sid = res["id"]
            elif isinstance(res, dict) and res.get("error"):
                logger.error(f"[SYNC] Create failed {sku}: {res['error']}")
                return None

        return {"resource_customs": {"sellasist_id": sid}} if sid else None

    async def handle_product_updated(self, sku, customs, events):
        if self.sync_mode == "create_only":
            logger.info(f"[SYNC] Skipping product_updated {sku} "
                        f"- mode is create_only")
            return None

        sid = _gc(customs, "sellasist_id")
        if not sid:
            ex = await self.sellasist.find_product_by_symbol(sku)
            if ex:
                sid = ex.get("id")
                logger.info(f"[SYNC] Found existing product {sku} -> {sid}")

        if not sid:
            if self.sync_mode == "create_and_update":
                logger.info(f"[SYNC] Product {sku} not found, "
                            f"falling back to create")
                return await self.handle_product_created(sku, customs, events)
            logger.info(f"[SYNC] Product {sku} not found, skipping update")
            return None

        data = self._extract(sku, events)
        logger.info(f"[SYNC] product_updated {sku}/{sid}: sending data={data}")

        res = await self.sellasist.update_product(int(sid), data)
        logger.info(f"[SYNC] Updated product {sku}/{sid}: {res}")
        return {"resource_customs": {"sellasist_id": sid}}

    async def handle_product_deleted(self, sku, customs):
        """Deactivate product instead of deleting - safer for e-commerce."""
        sid = _gc(customs, "sellasist_id")
        if sid:
            logger.info(f"[SYNC] Deactivating product {sku}/{sid}")
            await self.sellasist.update_product(int(sid), {"active": 0})
        else:
            logger.info(f"[SYNC] product_deleted {sku}: no sellasist_id, "
                        f"skipping")

    # -- Categories --------------------------------------------------------
    async def handle_category_created(self, code, customs, events):
        if not self.sync_categories:
            logger.info(f"[SYNC] Skipping category {code} - cats disabled")
            return None

        sid = _gc(customs, "sellasist_category_id")
        name = _etv(events, self.default_language) or code

        logger.info(f"[SYNC] category_created {code}: name={name} "
                    f"existing_id={sid}")

        if sid:
            await self.sellasist.update_category(int(sid), {"name": name})
        else:
            ex = await self.sellasist.find_category_by_name(name)
            if ex:
                sid = ex.get("id")
                logger.info(f"[SYNC] Found existing category {name} -> {sid}")
            else:
                res = await self.sellasist.create_category({"name": name})
                logger.info(f"[SYNC] Created category {name}: {res}")
                if isinstance(res, dict) and res.get("id"):
                    sid = res["id"]

        return {
            "resource_customs": {"sellasist_category_id": sid}
        } if sid else None

    async def handle_category_updated(self, code, customs, events):
        return await self.handle_category_created(code, customs, events)

    async def handle_category_deleted(self, code, customs):
        if not self.sync_categories:
            return None
        sid = _gc(customs, "sellasist_category_id")
        if sid:
            result = await self.sellasist.delete_category(int(sid))
            if isinstance(result, dict) and result.get("error"):
                logger.warning(
                    f"[SYNC] Category delete failed {code}: {result['error']}")
            else:
                logger.info(f"[SYNC] Category deleted: {code} -> {sid}")
        else:
            logger.info(f"[SYNC] Category delete skipped (no ID): {code}")
        return None

    # -- Extract -----------------------------------------------------------
    def _extract(self, sku: str, events: list) -> dict:
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
                    data.setdefault("additional_fields", {})[sf[3:]] = str(val)
                else:
                    data[sf] = str(val)
            except (ValueError, TypeError) as e:
                logger.warning(f"[SYNC] Convert error {ac}={val}->{sf}: {e}")

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


def _gc(customs, key):
    return customs.get(key) if customs and isinstance(customs, dict) else None


def _gtv(event, lang):
    for t in event.get("translations", []):
        if t.get("language") == lang:
            return t.get("value")
    tr = event.get("translations", [])
    return tr[0].get("value") if tr else event.get("value") or event.get("data")


def _etv(events, lang):
    for ev in events:
        val = _gtv(ev, lang)
        if val:
            return val
    return None
