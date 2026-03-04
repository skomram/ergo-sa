"""
Sellasist Connector for Ergonode Apps Engine v2
================================================
Version: 4.3.0

Changes v4.3.0:
  - Bidirectional sync: import from Sellasist to Ergonode via GraphQL
  - New config field: ergonode_api_key (X-API-KEY for GraphQL write access)
  - write_access: true in manifest
  - Import flow: Ergonode event -> read Sellasist -> write Ergonode GraphQL
  - Validation: import direction requires ergonode_api_key
  - compatible bumped to 4.2.0 to force reconfiguration
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
from ergonode_client import ErgonodeClient

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
    logger.info("Sellasist Connector v4.3.0 starting")
    yield
    logger.info("Sellasist Connector shutting down")

app = FastAPI(title="Sellasist Connector", version="4.3.0", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _get_claims(token: Optional[str]) -> dict:
    if not token:
        return {}
    return decode_jwt_unverified(token)


def _get_installation_id(claims: dict) -> str:
    return claims.get("app_installation_id", "unknown")


def _get_api_url(claims: dict) -> str:
    return claims.get("api_url", "") or claims.get("ergonode_api_url", "")


def _verify_request(token: Optional[str], store_ref: ConfigStore) -> tuple:
    if not token:
        return None, "unknown", JSONResponse(
            status_code=401,
            content={"error": "Missing X-APP-TOKEN header"})

    unverified = decode_jwt_unverified(token)
    iid = _get_installation_id(unverified)

    if iid == "unknown":
        return None, iid, JSONResponse(
            status_code=401,
            content={"error": "Missing app_installation_id in token"})

    inst = store_ref.get_installation(iid)
    if not inst or not inst.get("shared_secret"):
        logger.warning(f"[AUTH] No shared_secret for {iid}")
        return None, iid, JSONResponse(
            status_code=401,
            content={"error": "Unknown installation or missing secret"})

    verified = verify_jwt_signature(token, inst["shared_secret"])
    if verified is None:
        logger.warning(f"[AUTH] JWT verification failed for {iid}")
        return None, iid, JSONResponse(
            status_code=401,
            content={"error": "Invalid JWT signature"})

    return verified, iid, None


# ---------------------------------------------------------------------------
# GET / - Root
# ---------------------------------------------------------------------------
@app.get("/")
async def root():
    return {"app": "Sellasist Connector", "version": "4.3.0", "status": "ok"}


@app.head("/")
async def root_head():
    return Response(status_code=200)


# ---------------------------------------------------------------------------
# GET /manifest.json
# ---------------------------------------------------------------------------
@app.get("/manifest.json")
async def get_manifest():
    if not os.path.exists(MANIFEST_PATH):
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
# ---------------------------------------------------------------------------
@app.post("/handshake")
async def handshake(request: Request):
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
# ---------------------------------------------------------------------------
@app.get("/configuration")
async def get_configuration(
        request: Request,
        x_app_token: str = Header(None, alias="X-APP-TOKEN")):
    claims, iid, err = _verify_request(x_app_token, store)
    if err:
        return err

    inst = store.get_installation(iid)
    if not inst or "configuration" not in inst:
        return JSONResponse(status_code=200, content=[])

    return JSONResponse(status_code=200, content=inst["configuration"])


# ---------------------------------------------------------------------------
# POST /configuration - atomic per-step save
# ---------------------------------------------------------------------------
@app.post("/configuration")
async def post_configuration(
        request: Request,
        x_app_token: str = Header(None, alias="X-APP-TOKEN")):
    claims, iid, err = _verify_request(x_app_token, store)
    if err:
        return err

    body = await request.json()
    index = body.get("index", 0)
    config = body.get("configuration", {})

    logger.info(f"[CONFIG] step={index} installation={iid} "
                f"keys={list(config.keys())}")

    # --- Step 0: Validate connections ---
    if index == 0:
        host = config.get("shop_domain", "").strip()
        key = config.get("api_key", "").strip()
        erg_key = config.get("ergonode_api_key", "").strip()
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

        # Test Sellasist connection
        client = SellasistClient(api_key=key, shop_domain=host)
        ok, error_msg = await client.validate_connection()
        if not ok:
            return JSONResponse(status_code=422, content={
                "title": "Błąd połączenia Sellasist",
                "detail": error_msg,
                "violations": [{
                    "propertyPath": "api_key",
                    "title": error_msg,
                    "template": error_msg,
                    "parameters": {}
                }]
            })

        # Test Ergonode GraphQL connection (if key provided)
        if erg_key:
            api_url = _get_api_url(claims)
            if api_url:
                erg_client = ErgonodeClient(
                    api_url=api_url, api_key=erg_key)
                erg_ok, erg_err = await erg_client.test_connection()
                if not erg_ok:
                    return JSONResponse(status_code=422, content={
                        "title": "Błąd połączenia Ergonode GraphQL",
                        "detail": erg_err,
                        "violations": [{
                            "propertyPath": "ergonode_api_key",
                            "title": erg_err,
                            "template": erg_err,
                            "parameters": {}
                        }]
                    })
                logger.info(
                    f"[CONFIG] Ergonode GraphQL connection OK for {iid}")

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
        direction = config.get("syncDirection", "")
        lang = config.get("defaultLanguage", "")
        mode = config.get("syncMode", "")
        violations = []

        if not direction:
            violations.append({
                "propertyPath": "syncDirection",
                "title": "Wybierz kierunek synchronizacji",
                "template": "Wybierz kierunek synchronizacji",
                "parameters": {}
            })
        if not lang:
            violations.append({
                "propertyPath": "defaultLanguage",
                "title": "Wybierz język domyślny",
                "template": "Wybierz język domyślny",
                "parameters": {}
            })
        if not mode:
            violations.append({
                "propertyPath": "syncMode",
                "title": "Wybierz tryb synchronizacji",
                "template": "Wybierz tryb synchronizacji",
                "parameters": {}
            })
        if violations:
            return JSONResponse(status_code=422, content={
                "title": "Błąd walidacji",
                "detail": "Wypełnij wymagane pola",
                "violations": violations
            })

        # Warn if import selected but no Ergonode API key
        if direction == "sellasist_to_ergonode":
            inst = store.get_installation(iid) or {}
            configs = inst.get("configuration", [])
            step0 = configs[0] if configs else {}
            if not step0.get("ergonode_api_key"):
                logger.warning(
                    f"[CONFIG] Import direction without ergonode_api_key "
                    f"for {iid}")
                violations.append({
                    "propertyPath": "syncDirection",
                    "title": "Import wymaga klucza API Ergonode "
                             "(krok 1: Połączenie)",
                    "template": "Import wymaga klucza API Ergonode "
                                "(krok 1: Połączenie)",
                    "parameters": {}
                })
                return JSONResponse(status_code=422, content={
                    "title": "Brak klucza Ergonode",
                    "detail": "Import wymaga klucza API Ergonode "
                              "z uprawnieniem zapisu",
                    "violations": violations
                })

    # Persist - atomic per step
    store.update_config_step(iid, index, config)
    return JSONResponse(status_code=200, content={})


# ---------------------------------------------------------------------------
# GET /dictionary/{dictionary_id}
# ---------------------------------------------------------------------------
@app.get("/dictionary/{dictionary_id}")
async def get_dictionary(
        dictionary_id: str,
        request: Request,
        x_app_token: str = Header(None, alias="X-APP-TOKEN")):
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
# ---------------------------------------------------------------------------
@app.put("/consume/{event_name}")
async def consume_event(
        event_name: str,
        request: Request,
        x_app_token: str = Header(None, alias="X-APP-TOKEN")):
    body = await request.json()
    token = x_app_token

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
        return JSONResponse(status_code=200, content={})

    # -- app_uninstalled --
    if event_name == "app_uninstalled":
        store.remove_installation(iid)
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

    direction = sync_config.get("syncDirection", "ergonode_to_sellasist")

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
        logger.error(f"[CONSUME] Init error: {e}", exc_info=True)
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

    logger.info(
        f"[CONSUME] Processing {event_name}: resource={sku_or_code} "
        f"customs={customs is not None} events={len(events)} "
        f"direction={direction} mode={sync_config.get('syncMode')}")

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
            logger.info(
                f"[CONSUME] {event_name} {sku_or_code} -> {result}")
            return JSONResponse(status_code=200, content=result)

        return JSONResponse(status_code=200, content={})

    except Exception as e:
        logger.error(
            f"[CONSUME] Error {event_name} {sku_or_code}: {e}",
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
    return {"status": "ok", "version": "4.3.0"}


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
