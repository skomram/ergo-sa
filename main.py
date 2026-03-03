"""
Sellasist Connector for Ergonode Apps Engine v2
================================================
Version: 4.0.0

Endpoints per https://docs.ergonode.com/apps2/:
  GET  /                     - Root info (Render health check)
  GET  /manifest.json        - Serve manifest
  POST /handshake            - Receive shared_secret
  GET  /configuration        - Return saved configuration
  POST /configuration        - Validate & save configuration step
  GET  /dictionary/{id}      - Return dictionary data
  PUT  /event/{event}        - Handle app lifecycle events
  PUT  /consume/{event}      - Handle synchronization events
  GET  /health               - Health check
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
# Format: {"dictionary": [{"id": "...", "label": "..."}]}
# __skip__ allows user to skip mapping for fields they don't need.
SELLASIST_FIELDS = [
    {"id": "__skip__",       "label": "[Pomiń - nie synchronizuj]"},
    {"id": "name",           "label": "Nazwa produktu"},
    {"id": "description",    "label": "Opis produktu"},
    {"id": "price",          "label": "Cena sprzedaży"},
    {"id": "price_promo",    "label": "Cena promocyjna"},
    {"id": "price_buy",      "label": "Cena zakupu"},
    {"id": "quantity",       "label": "Stan magazynowy"},
    {"id": "ean",            "label": "Kod EAN"},
    {"id": "symbol",         "label": "Symbol (SKU)"},
    {"id": "active",         "label": "Aktywny (0/1)"},
    {"id": "weight",         "label": "Waga (kg)"},
    {"id": "category_id",    "label": "ID kategorii"},
    {"id": "pictures",       "label": "Zdjęcia"},
    {"id": "manufacturer",   "label": "Producent"},
    {"id": "catalog_number", "label": "Numer katalogowy"},
    {"id": "volume",         "label": "Objętość"},
    {"id": "location",       "label": "Lokalizacja"},
    {"id": "cf_1",           "label": "Pole dodatkowe 1"},
    {"id": "cf_2",           "label": "Pole dodatkowe 2"},
    {"id": "cf_3",           "label": "Pole dodatkowe 3"},
    {"id": "cf_4",           "label": "Pole dodatkowe 4"},
    {"id": "cf_5",           "label": "Pole dodatkowe 5"},
    {"id": "cf_6",           "label": "Pole dodatkowe 6"},
]

# ---------------------------------------------------------------------------
# App lifespan
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Sellasist Connector v4.0.0 starting")
    yield
    logger.info("Sellasist Connector shutting down")

app = FastAPI(title="Sellasist Connector", version="4.0.0", lifespan=lifespan)


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


# ---------------------------------------------------------------------------
# GET / - Root (Render health check needs 200 on /)
# ---------------------------------------------------------------------------
@app.get("/")
async def root():
    return {"app": "Sellasist Connector", "version": "4.0.0", "status": "ok"}


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
# ---------------------------------------------------------------------------
@app.post("/handshake")
async def handshake(request: Request):
    """
    Receive shared_secret from Ergonode on app installation.
    Body: {"shared_secret": "..."}
    X-APP-TOKEN header contains JWT with claims:
      app_installation_id, ergonode_api_url
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
    inst["ergonode_api_url"] = claims.get("ergonode_api_url", "")
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
    claims = _get_claims(x_app_token)
    iid = _get_installation_id(claims)
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
    body = await request.json()
    index = body.get("index", 0)
    config = body.get("configuration", {})

    claims = _get_claims(x_app_token)
    iid = _get_installation_id(claims)

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
        ok, err = await client.validate_connection()
        if not ok:
            return JSONResponse(status_code=422, content={
                "title": "Błąd połączenia",
                "detail": err,
                "violations": [{
                    "propertyPath": "api_key",
                    "title": err,
                    "template": err,
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
# Response: {"dictionary": [{"id": "...", "label": "..."}]}
# ---------------------------------------------------------------------------
@app.get("/dictionary/{dictionary_id}")
async def get_dictionary(
        dictionary_id: str,
        request: Request,
        x_app_token: str = Header(None, alias="X-APP-TOKEN")):
    claims = _get_claims(x_app_token)
    iid = _get_installation_id(claims)

    logger.info(f"[DICT] Requested: {dictionary_id} installation={iid}")

    if dictionary_id == "sellasist_fields":
        return JSONResponse(status_code=200, content={
            "dictionary": SELLASIST_FIELDS
        })

    logger.warning(f"[DICT] Unknown: {dictionary_id}")
    return JSONResponse(status_code=200, content={"dictionary": []})


# ---------------------------------------------------------------------------
# PUT /event/{event_name}
# Ref: https://docs.ergonode.com/apps2/detailed-reference/event-endpoints
# Events: app_installed, app_uninstalled
# ---------------------------------------------------------------------------
@app.put("/event/{event_name}")
async def handle_event(
        event_name: str,
        request: Request,
        x_app_token: str = Header(None, alias="X-APP-TOKEN")):
    body = await request.json()
    claims = _get_claims(x_app_token)
    iid = _get_installation_id(claims)

    logger.info(f"[EVENT] {event_name} installation={iid}")

    if event_name == "app_installed":
        inst = store.get_installation(iid) or {}
        inst["installed"] = True
        inst["ergonode_api_url"] = claims.get("ergonode_api_url", "")
        store.save_installation(iid, inst)

    elif event_name == "app_uninstalled":
        store.remove_installation(iid)

    return JSONResponse(status_code=200, content={})


# ---------------------------------------------------------------------------
# PUT /consume/{event_name}
# Ref: https://docs.ergonode.com/apps2/detailed-reference/synchronization
#
# Payload:
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
# Response 422 = error with retryable flag
# ---------------------------------------------------------------------------
@app.put("/consume/{event_name}")
async def consume_event(
        event_name: str,
        request: Request,
        x_app_token: str = Header(None, alias="X-APP-TOKEN")):
    body = await request.json()
    claims = _get_claims(x_app_token)
    iid = _get_installation_id(claims)

    logger.info(f"[CONSUME] {event_name} installation={iid}")

    # synchronization_ended
    if event_name == "synchronization_ended":
        sync_id = claims.get("synchronization_id", "?")
        logger.info(f"[CONSUME] Sync ended: sync_id={sync_id}")
        return JSONResponse(status_code=200, content={})

    # Load config
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
            ergonode_api_url=claims.get("ergonode_api_url", ""),
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
            logger.info(f"[CONSUME] Category deleted: {sku_or_code}")
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
    return {"status": "ok", "version": "4.0.0"}


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
