"""
Sellasist Connector for Ergonode Apps Engine v2
================================================
main.py - FastAPI application with all required endpoints.
Version: 3.4.0

Endpoints (as per https://docs.ergonode.com/apps2/):
  POST /handshake         - Receive shared_secret
  GET  /configuration     - Return saved configuration
  POST /configuration     - Validate & save configuration step
  GET  /dictionary/{id}   - Return dictionary data
  PUT  /consume/{event}   - Handle sync events
  PUT  /event/{event}     - Handle app lifecycle events
"""
import os
import json
import logging
import asyncio
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
# App lifespan
# ---------------------------------------------------------------------------
store = ConfigStore()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Sellasist Connector v3.4.0 starting")
    yield
    logger.info("Sellasist Connector shutting down")

app = FastAPI(title="Sellasist Connector", version="3.4.0", lifespan=lifespan)

# ---------------------------------------------------------------------------
# Sellasist fields dictionary
# ---------------------------------------------------------------------------
# Pole __skip__ pozwala uzytkownikowi "pominac" pole Sellasist w mapperze.
# Mapper widget wymusza mapowanie WSZYSTKICH pol z app dictionary,
# wiec __skip__ sluzy jako "nie synchronizuj tego pola".
SELLASIST_FIELDS_DICTIONARY = [
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


def _get_claims(x_app_token: Optional[str]) -> dict:
    """Decode JWT from X-APP-TOKEN header. Returns claims dict."""
    if not x_app_token:
        return {}
    return decode_jwt_unverified(x_app_token)


def _get_installation_id(claims: dict) -> str:
    """Extract installation ID from JWT claims."""
    return (claims.get("app_id")
            or claims.get("app_installation_id")
            or "unknown")


def _verified_claims(x_app_token: str, installation_id: str) -> Optional[dict]:
    """Verify JWT signature using stored shared_secret."""
    inst = store.get_installation(installation_id)
    if not inst:
        return None
    secret = inst.get("shared_secret", "")
    if not secret:
        return None
    return verify_jwt_signature(x_app_token, secret)


# ---------------------------------------------------------------------------
# POST /handshake
# ---------------------------------------------------------------------------
@app.post("/handshake")
async def handshake(request: Request):
    """
    Receive shared_secret from Ergonode during app installation.
    Ref: https://docs.ergonode.com/apps2/detailed-reference/authentication
    """
    body = await request.json()
    shared_secret = body.get("shared_secret", "")
    x_app_token = request.headers.get("X-APP-TOKEN", "")

    claims = _get_claims(x_app_token)
    installation_id = _get_installation_id(claims)

    logger.info(f"[HANDSHAKE] installation={installation_id}")

    inst_data = store.get_installation(installation_id) or {}
    inst_data["shared_secret"] = shared_secret
    inst_data["ergonode_api_url"] = claims.get("ergonode_api_url", "")
    inst_data["installation_id"] = installation_id
    store.save_installation(installation_id, inst_data)

    return JSONResponse(status_code=200, content={})


# ---------------------------------------------------------------------------
# GET /configuration
# ---------------------------------------------------------------------------
@app.get("/configuration")
async def get_configuration(request: Request,
                            x_app_token: str = Header(None, alias="X-APP-TOKEN")):
    """
    Return all persisted configuration steps.
    Ref: https://docs.ergonode.com/apps2/detailed-reference/configuration
    """
    claims = _get_claims(x_app_token)
    installation_id = _get_installation_id(claims)
    inst = store.get_installation(installation_id)

    if not inst or "configuration" not in inst:
        return JSONResponse(status_code=200, content=[])

    return JSONResponse(status_code=200, content=inst["configuration"])


# ---------------------------------------------------------------------------
# POST /configuration
# ---------------------------------------------------------------------------
@app.post("/configuration")
async def post_configuration(request: Request,
                             x_app_token: str = Header(None, alias="X-APP-TOKEN")):
    """
    Validate and save a configuration step.
    Body: {"index": 0, "configuration": {...}}
    Ref: https://docs.ergonode.com/apps2/detailed-reference/configuration
    """
    body = await request.json()
    index = body.get("index", 0)
    config = body.get("configuration", {})

    claims = _get_claims(x_app_token)
    installation_id = _get_installation_id(claims)

    logger.info(f"[CONFIG] step={index} installation={installation_id} "
                f"keys={list(config.keys())}")

    inst = store.get_installation(installation_id) or {}

    # --- Step 0: Validate Sellasist connection ---
    if index == 0:
        host = config.get("x_sa_host", "").strip()
        token = config.get("x_sa_token", "").strip()
        violations = []
        if not host:
            violations.append({
                "propertyPath": "x_sa_host",
                "title": "Domena sklepu jest wymagana",
                "template": "Domena sklepu jest wymagana",
                "parameters": {}
            })
        if not token:
            violations.append({
                "propertyPath": "x_sa_token",
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

        # Validate actual API connection
        client = SellasistClient(api_key=token, shop_domain=host)
        ok, err = await client.validate_connection()
        if not ok:
            return JSONResponse(status_code=422, content={
                "title": "Błąd połączenia",
                "detail": err,
                "violations": [{
                    "propertyPath": "x_sa_token",
                    "title": err,
                    "template": err,
                    "parameters": {}
                }]
            })

    # --- Step 1: Attribute mapping (validated by Ergonode mapper widget) ---
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
        # Count non-skip mappings
        real_mappings = [m for m in mapping
                         if m.get("app") != "__skip__"
                         and m.get("ergonode")]
        if not real_mappings:
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
        logger.info(f"[CONFIG] Mapping: {len(real_mappings)} real, "
                     f"{len(mapping) - len(real_mappings)} skipped")

    # --- Step 2: Sync settings (basic validation) ---
    if index == 2:
        lang = config.get("defaultLanguage", "")
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

    # Persist configuration step
    configs = inst.get("configuration", [])
    while len(configs) <= index:
        configs.append({})
    configs[index] = config
    inst["configuration"] = configs
    store.save_installation(installation_id, inst)

    return JSONResponse(status_code=200, content={})


# ---------------------------------------------------------------------------
# GET /dictionary/{dictionary_id}
# ---------------------------------------------------------------------------
@app.get("/dictionary/{dictionary_id}")
async def get_dictionary(dictionary_id: str, request: Request,
                         x_app_token: str = Header(None, alias="X-APP-TOKEN")):
    """
    Return dictionary data for configuration widgets.
    Ref: https://docs.ergonode.com/apps2/detailed-reference/manifest/dictionaries
    """
    claims = _get_claims(x_app_token)
    installation_id = _get_installation_id(claims)

    logger.info(f"[DICT] Requested: {dictionary_id} "
                f"installation={installation_id}")

    if dictionary_id == "sellasist_fields":
        return JSONResponse(status_code=200, content={
            "dictionary": SELLASIST_FIELDS_DICTIONARY
        })

    # Unknown dictionary
    logger.warning(f"[DICT] Unknown dictionary: {dictionary_id}")
    return JSONResponse(status_code=200, content={"dictionary": []})


# ---------------------------------------------------------------------------
# PUT /event/{event_name} - App lifecycle events
# ---------------------------------------------------------------------------
@app.put("/event/{event_name}")
async def handle_event(event_name: str, request: Request,
                       x_app_token: str = Header(None, alias="X-APP-TOKEN")):
    """
    Handle app lifecycle events (app_installed, app_uninstalled).
    Ref: https://docs.ergonode.com/apps2/detailed-reference/event-endpoints
    """
    body = await request.json()
    claims = _get_claims(x_app_token)
    installation_id = _get_installation_id(claims)

    logger.info(f"[EVENT] {event_name} installation={installation_id}")

    if event_name == "app_installed":
        inst = store.get_installation(installation_id) or {}
        inst["installed"] = True
        inst["ergonode_api_url"] = claims.get("ergonode_api_url", "")
        store.save_installation(installation_id, inst)

    elif event_name == "app_uninstalled":
        store.remove_installation(installation_id)

    return JSONResponse(status_code=200, content={})


# ---------------------------------------------------------------------------
# PUT /consume/{event_name} - Synchronization events
# ---------------------------------------------------------------------------
@app.put("/consume/{event_name}")
async def consume_event(event_name: str, request: Request,
                        x_app_token: str = Header(None, alias="X-APP-TOKEN")):
    """
    Handle synchronization events from Ergonode.
    Ref: https://docs.ergonode.com/apps2/detailed-reference/synchronization

    Payload structure:
    {
      "name": "product_created",
      "resource_id": {"id": "SKU", "type": "sku"},
      "synchronization": {
        "resource_customs": {...} | null,
        "events": [...]
      }
    }
    """
    body = await request.json()
    claims = _get_claims(x_app_token)
    installation_id = _get_installation_id(claims)

    logger.info(f"[CONSUME] {event_name} installation={installation_id}")

    # --- synchronization_ended ---
    if event_name == "synchronization_ended":
        sync_id = claims.get("synchronization_id", "?")
        logger.info(f"[CONSUME] Sync ended: sync_id={sync_id}")
        return JSONResponse(status_code=200, content={})

    # --- Load installation config ---
    inst = store.get_installation(installation_id)
    if not inst or "configuration" not in inst:
        logger.error(f"[CONSUME] No config for {installation_id}")
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
            installation_id=installation_id,
        )
    except Exception as e:
        logger.error(f"[CONSUME] SyncHandler init error: {e}")
        return JSONResponse(status_code=422, content={
            "title": "Błąd inicjalizacji",
            "detail": str(e)[:256],
            "violations": [],
            "retryable": True
        })

    # Extract common payload fields
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
            logger.warning(f"[CONSUME] Unknown event: {event_name}")

        # Return resource_customs if handler provided them
        if result and isinstance(result, dict):
            return JSONResponse(status_code=200, content=result)

        return JSONResponse(status_code=200, content={})

    except Exception as e:
        logger.error(f"[CONSUME] Error: {event_name} {sku_or_code}: {e}",
                     exc_info=True)
        return JSONResponse(status_code=422, content={
            "title": "Błąd synchronizacji",
            "detail": str(e)[:256],
            "violations": [],
            "retryable": True
        })


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
@app.get("/health")
async def health():
    return {"status": "ok", "version": "3.4.0"}


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
