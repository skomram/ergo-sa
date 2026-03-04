"""
Sellasist Connector for Ergonode Apps Engine v2
================================================
Version: 4.1.0

Endpoints per https://docs.ergonode.com/apps2/:
  GET  /                     - Root info (Render health check)
  GET  /manifest.json        - Serve manifest
  POST /handshake            - Receive shared_secret
  GET  /configuration        - Return saved configuration
  POST /configuration        - Validate & save configuration step
  GET  /dictionary/{id}      - Return dictionary data
  PUT  /consume/{event}      - Handle all events (lifecycle + sync)
  GET  /health               - Health check

Changes v4.1.0:
  - JWT signature verification on all authenticated endpoints
  - Removed dead /event/ endpoint (v2 uses /consume/ for everything)
  - Fixed JWT claim name: api_url (with ergonode_api_url fallback)
  - Added synchronization_scheduler feature
  - Added type field to dictionary entries for mapper validation
  - Added allows_merging for pictures dictionary entry
  - Added category_deleted handling
  - Encrypted shared_secret at rest (via config_store)
  - Added icon to manifest
"""
import os
import json
import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Request, Response, Header
from fastapi.responses import JSONResponse

from auth import decode_jwt_unverified, verify_jwt_signature
from config_store import ConfigStore
from sync_handler import SyncHandler
from sellasist_client import SellasistClient

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("sellasist-app")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
MANIFEST_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "manifest.json")

store = ConfigStore()

# Dictionary: app:sellasist_fields
# Ref: https://docs.ergonode.com/apps2/detailed-reference/manifest/dictionaries
# Format: {"dictionary": [{"id": "...", "label": "...", "type": "...", ...}]}
#
# type field enables mapper type validation:
#   Ergonode attribute types mapped to Sellasist field compatibility.
#   Multiple types separated with |
#
# allows_merging: when true, multiple Ergonode attributes can map to
#   the same Sellasist field (merged by App logic)
#
# __skip__ allows user to skip mapping for fields they don't need.
SELLASIST_FIELDS = [
    {"id": "__skip__",       "label": "[Pomiń - nie synchronizuj]"},
    {"id": "name",           "label": "Nazwa produktu",
     "type": "TEXT|TEXT_AREA"},
    {"id": "description",    "label": "Opis produktu",
     "type": "TEXT_AREA|TEXT", "allows_merging": True},
    {"id": "price",          "label": "Cena sprzedaży",
     "type": "NUMERIC|PRICE|UNIT"},
    {"id": "price_promo",    "label": "Cena promocyjna",
     "type": "NUMERIC|PRICE|UNIT"},
    {"id": "price_buy",      "label": "Cena zakupu",
     "type": "NUMERIC|PRICE|UNIT"},
    {"id": "quantity",       "label": "Stan magazynowy",
     "type": "NUMERIC"},
    {"id": "ean",            "label": "Kod EAN",
     "type": "TEXT"},
    {"id": "symbol",         "label": "Symbol (SKU)",
     "type": "TEXT"},
    {"id": "active",         "label": "Aktywny (0/1)",
     "type": "NUMERIC|SELECT"},
    {"id": "weight",         "label": "Waga (kg)",
     "type": "NUMERIC|UNIT"},
    {"id": "category_id",    "label": "ID kategorii",
     "type": "NUMERIC|SELECT"},
    {"id": "pictures",       "label": "Zdjęcia",
     "type": "IMAGE|GALLERY", "allows_merging": True},
    {"id": "manufacturer",   "label": "Producent",
     "type": "TEXT|SELECT"},
    {"id": "catalog_number", "label": "Numer katalogowy",
     "type": "TEXT"},
    {"id": "volume",         "label": "Objętość",
     "type": "NUMERIC|UNIT"},
    {"id": "location",       "label": "Lokalizacja",
     "type": "TEXT"},
    {"id": "cf_1",           "label": "Pole dodatkowe 1",
     "type": "TEXT|TEXT_AREA|NUMERIC|SELECT|MULTI_SELECT|DATE"},
    {"id": "cf_2",           "label": "Pole dodatkowe 2",
     "type": "TEXT|TEXT_AREA|NUMERIC|SELECT|MULTI_SELECT|DATE"},
    {"id": "cf_3",           "label": "Pole dodatkowe 3",
     "type": "TEXT|TEXT_AREA|NUMERIC|SELECT|MULTI_SELECT|DATE"},
    {"id": "cf_4",           "label": "Pole dodatkowe 4",
     "type": "TEXT|TEXT_AREA|NUMERIC|SELECT|MULTI_SELECT|DATE"},
    {"id": "cf_5",           "label": "Pole dodatkowe 5",
     "type": "TEXT|TEXT_AREA|NUMERIC|SELECT|MULTI_SELECT|DATE"},
    {"id": "cf_6",           "label": "Pole dodatkowe 6",
     "type": "TEXT|TEXT_AREA|NUMERIC|SELECT|MULTI_SELECT|DATE"},
]

# ---------------------------------------------------------------------------
# App lifespan
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Sellasist Connector v4.1.0 starting")
    yield
    logger.info("Sellasist Connector shutting down")

app = FastAPI(title="Sellasist Connector", version="4.1.0", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _get_claims(token: Optional[str]) -> dict:
    """Decode JWT from X-APP-TOKEN header without verification."""
    if not token:
        return {}
    return decode_jwt_unverified(token)


def _get_installation_id(claims: dict) -> str:
    """
    Extract installation ID from JWT claims.
    Ref: https://docs.ergonode.com/apps2/detailed-reference/authentication
    Claim name: app_installation_id
    """
    return claims.get("app_installation_id", "unknown")


def _get_api_url(claims: dict) -> str:
    """
    Extract Ergonode API URL from JWT claims.
    Docs reference: api_url
    Fallback: ergonode_api_url (for backward compat with older Ergonode)
    """
    return claims.get("api_url", "") or claims.get("ergonode_api_url", "")


def _verify_request(token: Optional[str], store_ref: ConfigStore) -> tuple:
    """
    Full JWT authentication flow per Ergonode docs:
    1. Decode without verification to get app_installation_id
    2. Look up shared_secret from store
    3. Verify JWT signature with HMAC SHA-256

    Returns: (verified_claims, installation_id, error_response)
    If error_response is not None, return it immediately.
    """
    if not token:
        return None, "unknown", JSONResponse(
            status_code=401,
            content={"error": "Missing X-APP-TOKEN header"})

    # Step 1: extract installation_id without verification
    unverified = decode_jwt_unverified(token)
    iid = _get_installation_id(unverified)

    if iid == "unknown":
        return None, iid, JSONResponse(
            status_code=401,
            content={"error": "Missing app_installation_id in token"})

    # Step 2: look up shared_secret
    inst = store_ref.get_installation(iid)
    if not inst or not inst.get("shared_secret"):
        logger.warning(f"[AUTH] No shared_secret for {iid}")
        return None, iid, JSONResponse(
            status_code=401,
            content={"error": "Unknown installation or missing secret"})

    # Step 3: verify signature
    verified = verify_jwt_signature(token, inst["shared_secret"])
    if verified is None:
        logger.warning(f"[AUTH] JWT verification failed for {iid}")
        return None, iid, JSONResponse(
            status_code=401,
            content={"error": "Invalid JWT signature"})

    return verified, iid, None


# ---------------------------------------------------------------------------
# GET / - Root (Render health check needs 200 on /)
# ---------------------------------------------------------------------------
@app.get("/")
async def root():
    return {"app": "Sellasist Connector", "version": "4.1.0", "status": "ok"}


@app.head("/")
async def root_head():
    return Response(status_code=200)


# ---------------------------------------------------------------------------
# GET /manifest.json - Serve manifest for Ergonode registration
# ---------------------------------------------------------------------------
@app.get("/manifest.json")
async def get_manifest():
    if not os.path.exists(MANIFEST_PATH):
        logger.error(f"[MANIFEST] Not found: {MANIFEST_PATH}")
        return JSONResponse(status_code=404,
                            content={"error": "manifest.json not found"})
    with open(MANIFEST_PATH) as f:
        data = json.load(f)
    return JSONResponse(status_code=200, content=data,
                        media_type="application/json")


@app.head("/manifest.json")
async def manifest_head():
    if os.path.exists(MANIFEST_PATH):
        return Response(status_code=200)
    return Response(status_code=404)


# ---------------------------------------------------------------------------
# POST /handshake
# Ref: https://docs.ergonode.com/apps2/detailed-reference/authentication
#
# This is the ONLY endpoint that cannot verify JWT signature
# because we don't yet have the shared_secret.
# ---------------------------------------------------------------------------
@app.post("/handshake")
async def handshake(request: Request):
    """
    Receive shared_secret from Ergonode on app installation.
    Body: {"shared_secret": "..."}
    X-APP-TOKEN header contains JWT with claims:
      app_installation_id, api_url
    Response: 2xx = success
    """
    body = await request.json()
    shared_secret = body.get("shared_secret", "")
    token = request.headers.get("X-APP-TOKEN", "")

    claims = _get_claims(token)
    iid = _get_installation_id(claims)

    logger.info(f"[HANDSHAKE] installation={iid}")

    inst = store.get_installation(iid) or {}
    inst["shared_secret"] = shared_secret
    inst["api_url"] = _get_api_url(claims)
    inst["installation_id"] = iid
    store.save_installation(iid, inst)

    return JSONResponse(status_code=200, content={})


# ---------------------------------------------------------------------------
# GET /configuration
# Ref: https://docs.ergonode.com/apps2/detailed-reference/configuration
# Response: array of config objects, one per step
# ---------------------------------------------------------------------------
@app.get("/configuration")
async def get_configuration(
        request: Request,
        x_app_token: str = Header(None, alias="X-APP-TOKEN")):
    # Verify JWT
    claims, iid, err = _verify_request(x_app_token, store)
    if err:
        return err

    inst = store.get_installation(iid)
    if not inst or "configuration" not in inst:
        return JSONResponse(status_code=200, content=[])

    return JSONResponse(status_code=200, content=inst["configuration"])


# ---------------------------------------------------------------------------
# POST /configuration
# Ref: https://docs.ergonode.com/apps2/detailed-reference/configuration
# Body: {"index": 0, "configuration": {...}}
# Response: 2xx = success, 422 = validation error with violations
# ---------------------------------------------------------------------------
@app.post("/configuration")
async def post_configuration(
        request: Request,
        x_app_token: str = Header(None, alias="X-APP-TOKEN")):
    # Verify JWT
    claims, iid, err = _verify_request(x_app_token, store)
    if err:
        return err

    body = await request.json()
    index = body.get("index", 0)
    config = body.get("configuration", {})

    logger.info(f"[CONFIG] step={index} installation={iid} "
                f"keys={list(config.keys())}")

    inst = store.get_installation(iid) or {}

    # --- Step 0: Validate Sellasist connection ---
    if index == 0:
        host = config.get("shop_domain", "").strip()
        key = config.get("api_key", "").strip()
        violations = []
        if not host:
            violations.append({
                "propertyPath": "shop_domain",
                "title": "Domena sklepu jest wymagana",
                "template": "Domena sklepu jest wymagana",
                "parameters": {}
            })
        if not key:
            violations.append({
                "propertyPath": "api_key",
                "title": "Klucz API jest wymagany",
                "template": "Klucz API jest wymagany",
                "parameters": {}
            })
        if violations:
            return JSONResponse(status_code=422, content={
                "title": "Błędy walidacji",
                "detail": "Wypełnij wymagane pola",
                "violations": violations
            })

        # Test actual connection
        client = SellasistClient(api_key=key, shop_domain=host)
        ok, error_msg = await client.validate_connection()
        if not ok:
            return JSONResponse(status_code=422, content={
                "title": "Błąd połączenia",
                "detail": error_msg,
                "violations": [{
                    "propertyPath": "api_key",
                    "title": error_msg,
                    "template": error_msg,
                    "parameters": {}
                }]
            })

    # --- Step 1: Attribute mapping ---
    if index == 1:
        mapping = config.get("attributeMapping", [])
        if not mapping:
            return JSONResponse(status_code=422, content={
                "title": "Błąd mapowania",
                "detail": "Mapowanie atrybutów jest wymagane",
                "violations": [{
                    "propertyPath": "attributeMapping",
                    "title": "Zmapuj przynajmniej jeden atrybut",
                    "template": "Zmapuj przynajmniej jeden atrybut",
                    "parameters": {}
                }]
            })
        real = [m for m in mapping
                if m.get("app") != "__skip__" and m.get("ergonode")]
        if not real:
            return JSONResponse(status_code=422, content={
                "title": "Błąd mapowania",
                "detail": "Przynajmniej jedno pole musi być zmapowane",
                "violations": [{
                    "propertyPath": "attributeMapping",
                    "title": "Zmapuj przynajmniej jeden atrybut",
                    "template": "Zmapuj przynajmniej jeden atrybut",
                    "parameters": {}
                }]
            })
        logger.info(f"[CONFIG] Mapping: {len(real)} real, "
                     f"{len(mapping) - len(real)} skipped")

    # --- Step 2: Sync settings ---
    if index == 2:
        lang = config.get("defaultLanguage", "")
        mode = config.get("syncMode", "")
        if not lang:
            return JSONResponse(status_code=422, content={
                "title": "Błąd walidacji",
                "detail": "Język domyślny jest wymagany",
                "violations": [{
                    "propertyPath": "defaultLanguage",
                    "title": "Wybierz język domyślny",
                    "template": "Wybierz język domyślny",
                    "parameters": {}
                }]
            })
        if not mode:
            return JSONResponse(status_code=422, content={
                "title": "Błąd walidacji",
                "detail": "Tryb synchronizacji jest wymagany",
                "violations": [{
                    "propertyPath": "syncMode",
                    "title": "Wybierz tryb synchronizacji",
                    "template": "Wybierz tryb synchronizacji",
                    "parameters": {}
                }]
            })

    # Persist
    configs = inst.get("configuration", [])
    while len(configs) <= index:
        configs.append({})
    configs[index] = config
    inst["configuration"] = configs
    store.save_installation(iid, inst)

    return JSONResponse(status_code=200, content={})


# ---------------------------------------------------------------------------
# GET /dictionary/{dictionary_id}
# Ref: https://docs.ergonode.com/apps2/detailed-reference/manifest/dictionaries
# Response: {"dictionary": [{"id": "...", "label": "...", "type": "..."}]}
# ---------------------------------------------------------------------------
@app.get("/dictionary/{dictionary_id}")
async def get_dictionary(
        dictionary_id: str,
        request: Request,
        x_app_token: str = Header(None, alias="X-APP-TOKEN")):
    # Verify JWT
    claims, iid, err = _verify_request(x_app_token, store)
    if err:
        return err

    logger.info(f"[DICT] Requested: {dictionary_id} installation={iid}")

    if dictionary_id == "sellasist_fields":
        return JSONResponse(status_code=200, content={
            "dictionary": SELLASIST_FIELDS
        })

    logger.warning(f"[DICT] Unknown: {dictionary_id}")
    return JSONResponse(status_code=200, content={"dictionary": []})


# ---------------------------------------------------------------------------
# PUT /consume/{event_name}
# Ref: https://docs.ergonode.com/apps2/detailed-reference/event-endpoints
# Ref: https://docs.ergonode.com/apps2/detailed-reference/synchronization
#
# In Apps Engine v2, ALL events (lifecycle + sync) come through /consume/
#
# Lifecycle events:
#   app_installed, app_uninstalled
#
# Sync events payload:
# {
#   "name": "product_created",
#   "resource_id": {"id": "SKU", "type": "sku"},
#   "synchronization": {
#     "resource_customs": {...} | null,
#     "events": [...]
#   }
# }
#
# Response 2xx = success (optionally with resource_customs)
# Response 204 = success without persisting resource_customs
# Response 422 = error with retryable flag
# ---------------------------------------------------------------------------
@app.put("/consume/{event_name}")
async def consume_event(
        event_name: str,
        request: Request,
        x_app_token: str = Header(None, alias="X-APP-TOKEN")):
    body = await request.json()
    token = x_app_token

    # For app_installed we may not have shared_secret yet if handshake
    # just happened, so use unverified claims for lifecycle events.
    # For sync events, we verify JWT fully.
    if event_name in ("app_installed", "app_uninstalled"):
        claims = _get_claims(token)
        iid = _get_installation_id(claims)
    else:
        claims, iid, err = _verify_request(token, store)
        if err:
            return err

    logger.info(f"[CONSUME] {event_name} installation={iid}")

    # -- synchronization_ended --
    if event_name == "synchronization_ended":
        sync_id = claims.get("synchronization_id", "?")
        logger.info(f"[CONSUME] Sync ended: sync_id={sync_id}")
        return JSONResponse(status_code=200, content={})

    # -- app_installed --
    if event_name == "app_installed":
        inst = store.get_installation(iid) or {}
        inst["installed"] = True
        inst["api_url"] = _get_api_url(claims)
        store.save_installation(iid, inst)
        logger.info(f"[CONSUME] app_installed OK for {iid}")
        return JSONResponse(status_code=200, content={})

    # -- app_uninstalled --
    if event_name == "app_uninstalled":
        store.remove_installation(iid)
        logger.info(f"[CONSUME] app_uninstalled for {iid}")
        return JSONResponse(status_code=200, content={})

    # -- Sync events: load config --
    inst = store.get_installation(iid)
    if not inst or "configuration" not in inst:
        logger.error(f"[CONSUME] No config for {iid}")
        return JSONResponse(status_code=422, content={
            "title": "Brak konfiguracji",
            "detail": "Aplikacja nie została skonfigurowana",
            "violations": [],
            "retryable": False
        })

    configs = inst["configuration"]
    sellasist_config = configs[0] if len(configs) > 0 else {}
    mapping_config = configs[1] if len(configs) > 1 else {}
    sync_config = configs[2] if len(configs) > 2 else {}

    try:
        handler = SyncHandler(
            sellasist_config=sellasist_config,
            mapping_config=mapping_config,
            sync_config=sync_config,
            ergonode_api_url=_get_api_url(claims),
            shared_secret=inst.get("shared_secret", ""),
            installation_id=iid,
        )
    except Exception as e:
        logger.error(f"[CONSUME] Init error: {e}")
        return JSONResponse(status_code=422, content={
            "title": "Błąd inicjalizacji",
            "detail": str(e)[:256],
            "violations": [],
            "retryable": True
        })

    resource_id = body.get("resource_id", {})
    sku_or_code = resource_id.get("id", "")
    sync_data = body.get("synchronization", {})
    customs = sync_data.get("resource_customs")
    events = sync_data.get("events", [])

    try:
        result = None

        if event_name == "product_created":
            result = await handler.handle_product_created(
                sku_or_code, customs, events)
        elif event_name == "product_updated":
            result = await handler.handle_product_updated(
                sku_or_code, customs, events)
        elif event_name == "product_deleted":
            await handler.handle_product_deleted(sku_or_code, customs)
        elif event_name == "category_created":
            result = await handler.handle_category_created(
                sku_or_code, customs, events)
        elif event_name == "category_updated":
            result = await handler.handle_category_updated(
                sku_or_code, customs, events)
        elif event_name == "category_deleted":
            await handler.handle_category_deleted(sku_or_code, customs)
        else:
            logger.warning(f"[CONSUME] Unknown: {event_name}")

        if result and isinstance(result, dict):
            return JSONResponse(status_code=200, content=result)
        return JSONResponse(status_code=200, content={})

    except Exception as e:
        logger.error(f"[CONSUME] Error {event_name} {sku_or_code}: {e}",
                     exc_info=True)
        return JSONResponse(status_code=422, content={
            "title": "Błąd synchronizacji",
            "detail": str(e)[:256],
            "violations": [],
            "retryable": True
        })


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------
@app.get("/health")
async def health():
    return {"status": "ok", "version": "4.1.0"}


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
