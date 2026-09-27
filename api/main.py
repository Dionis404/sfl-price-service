"""
API-сервис истории цен ресурсов Sunflower Land.

Читает из той же таблицы, куда пишет collector (sfl_resource_prices).
Ничего сам не создаёт и не опрашивает — чисто отдача данных наружу
(для сайта / фронтенда / любого другого потребителя).

Переменные окружения:
    DATABASE_URL    обязательно, напр. postgresql://user:pass@host:5432/sfl
    CORS_ORIGINS    опционально, через запятую, по умолчанию "*"
"""

import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

DATABASE_URL = os.environ.get("DATABASE_URL")
CORS_ORIGINS = [o.strip() for o in os.environ.get("CORS_ORIGINS", "*").split(",")]

if not DATABASE_URL:
    raise RuntimeError("Нужна переменная окружения DATABASE_URL")

app = FastAPI(
    title="SFL Price History API",
    description="История цен всех торгуемых предметов Sunflower Land: collectibles (ресурсы, культуры, крафт-материалы, эмблемы, редкие коллекционки), wearables, buds, pets, economies.",
    version="1.1.0",
)

CATEGORIES = ("collectibles", "wearables", "buds", "pets", "economies")

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_methods=["GET"],
    allow_headers=["*"],
)


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def row_to_dict(row) -> dict:
    d = dict(row)
    if d.get("floor") is not None and d.get("flower_price") is not None:
        d["floor_usd"] = float(d["floor"]) * float(d["flower_price"])
    if d.get("latest_sale") is not None and d.get("flower_price") is not None:
        d["latest_sale_usd"] = float(d["latest_sale"]) * float(d["flower_price"])
    if "captured_at" in d and isinstance(d["captured_at"], datetime):
        d["captured_at"] = d["captured_at"].isoformat()
    if "snapshot_date" in d and d["snapshot_date"] is not None:
        d["snapshot_date"] = str(d["snapshot_date"])
    return d


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/resources")
def list_resources(
    category: Optional[str] = Query(
        None, description=f"Фильтр по префиксу item_key: {', '.join(CATEGORIES)}"
    ),
    limit: int = Query(5000, ge=1, le=20000),
    offset: int = Query(0, ge=0),
):
    """Список отслеживаемых предметов (item_key + имя), по данным в БД.
    Без category вернёт всё — это может быть несколько тысяч строк
    (collectibles + wearables + buds + pets + economies)."""
    where = ""
    params: list = []
    if category:
        where = "WHERE item_key LIKE %s"
        params.append(f"{category}-%")
    params.extend([limit, offset])
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT DISTINCT item_key, item_name
                FROM sfl_resource_prices
                {where}
                ORDER BY item_name
                LIMIT %s OFFSET %s
                """,
                params,
            )
            return [dict(r) for r in cur.fetchall()]


@app.get("/prices/latest")
def latest_prices(
    category: Optional[str] = Query(
        None, description=f"Фильтр по префиксу item_key: {', '.join(CATEGORIES)}"
    ),
    limit: int = Query(5000, ge=1, le=20000),
    offset: int = Query(0, ge=0),
):
    """Последний известный снимок цены по каждому предмету.
    Без category — по ВСЕМ отслеживаемым предметам (тысячи строк);
    используй category, если нужен только один раздел (напр. только
    collectibles — базовые ресурсы и крафт-материалы)."""
    where = ""
    params: list = []
    if category:
        where = "WHERE item_key LIKE %s"
        params.append(f"{category}-%")
    params.extend([limit, offset])
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT DISTINCT ON (item_key)
                    item_key, item_name, captured_at, snapshot_date,
                    floor, low, high, latest_sale, best_offer,
                    volume, trades, quantity, listing_count, offer_count,
                    flower_price
                FROM sfl_resource_prices
                {where}
                ORDER BY item_key, captured_at DESC
                LIMIT %s OFFSET %s
                """,
                params,
            )
            return [row_to_dict(r) for r in cur.fetchall()]


@app.get("/prices/{item_key}/latest")
def latest_price_for_item(item_key: str):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT item_key, item_name, captured_at, snapshot_date,
                       floor, low, high, latest_sale, best_offer,
                       volume, trades, quantity, listing_count, offer_count,
                       flower_price
                FROM sfl_resource_prices
                WHERE item_key = %s
                ORDER BY captured_at DESC
                LIMIT 1
                """,
                (item_key,),
            )
            row = cur.fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail=f"Нет данных по {item_key}")
            return row_to_dict(row)


@app.get("/prices/{item_key}/history")
def price_history(
    item_key: str,
    hours: int = Query(24, ge=1, le=24 * 90, description="За сколько последних часов вернуть историю"),
    limit: int = Query(2000, ge=1, le=20000),
):
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT item_key, item_name, captured_at, snapshot_date,
                       floor, low, high, latest_sale, best_offer,
                       volume, trades, quantity, listing_count, offer_count,
                       flower_price
                FROM sfl_resource_prices
                WHERE item_key = %s AND captured_at >= %s
                ORDER BY captured_at ASC
                LIMIT %s
                """,
                (item_key, since, limit),
            )
            rows = cur.fetchall()
            if not rows:
                raise HTTPException(status_code=404, detail=f"Нет данных по {item_key} за последние {hours}ч")
            return [row_to_dict(r) for r in rows]
