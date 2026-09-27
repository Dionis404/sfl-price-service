#!/usr/bin/env python3
"""
collector.py

Постоянный сервис (контейнер с restart:always): раз в POLL_INTERVAL_SECONDS
опрашивает community API Sunflower Land (marketplaceActivity) и пишет в
Postgres цены ВСЕХ торгуемых предметов — collectibles, wearables, buds,
pets, economies (портальные мини-игры). Ничего не фильтрует по списку,
берёт всё, что реально есть в ответе API на текущий день.

Чтобы не заливать БД дублями (у большинства редких предметов цена не
меняется по многу часов), пишет строку только когда хоть одно
отслеживаемое поле реально изменилось с прошлого известного значения
(change-log, не снапшот-на-каждый-опрос). При старте подтягивает
последнее известное состояние каждого предмета из самой БД — так что
перезапуск контейнера не создаёт лавину "всё изменилось".

При старте сам создаёт таблицу sfl_resource_prices, если её ещё нет —
никаких ручных миграций делать не нужно, только указать DATABASE_URL.

Переменные окружения:
    SFL_API_KEY            обязательно
    DATABASE_URL            обязательно, напр. postgresql://user:pass@host:5432/sfl
    POLL_INTERVAL_SECONDS   опционально, по умолчанию 300 (5 минут)
"""

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests

API_URL = "https://api.sunflower-land.com/community/data"
SCRIPT_DIR = Path(__file__).resolve().parent
ITEM_NAMES_FILE = SCRIPT_DIR / "item_names.json"

# Поля, по которым отслеживаем изменение (если хоть одно из них не
# совпадает с прошлым известным значением — пишем новую строку)
TRACKED_FIELDS = [
    "floor", "low", "high", "latestSale", "bestOffer",
    "volume", "trades", "quantity", "listingCount", "offerCount",
]

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS sfl_resource_prices (
    id              BIGSERIAL PRIMARY KEY,
    captured_at     TIMESTAMPTZ NOT NULL,
    snapshot_date   DATE        NOT NULL,
    item_key        TEXT        NOT NULL,
    item_name       TEXT        NOT NULL,
    floor           NUMERIC,
    low             NUMERIC,
    high            NUMERIC,
    latest_sale     NUMERIC,
    best_offer      NUMERIC,
    volume          NUMERIC,
    trades          BIGINT,
    quantity        NUMERIC,
    listing_count   INTEGER,
    offer_count     INTEGER,
    flower_price    NUMERIC     NOT NULL,
    UNIQUE (item_key, captured_at)
);
CREATE INDEX IF NOT EXISTS idx_sfl_resource_prices_item_time
    ON sfl_resource_prices (item_key, captured_at DESC);
CREATE INDEX IF NOT EXISTS idx_sfl_resource_prices_date
    ON sfl_resource_prices (snapshot_date);
"""

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("sfl-collector")

_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    log.info("Получен сигнал %s, завершаюсь после текущего опроса...", signum)
    _shutdown = True


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


def today_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def load_item_names() -> dict:
    """Известные имена (сейчас — только collectibles-*, из KNOWN_IDS игры).
    Для wearables/buds/pets/economies такого маппинга нет — используем
    сам item_key как имя."""
    if ITEM_NAMES_FILE.exists():
        with ITEM_NAMES_FILE.open(encoding="utf-8") as f:
            return json.load(f)
    return {}


def ensure_schema(conn):
    with conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)


def load_last_state(conn) -> dict:
    """Последнее известное состояние каждого предмета — чтобы после
    рестарта контейнера не считать всё "изменившимся"."""
    state = {}
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (item_key)
                item_key, floor, low, high, latest_sale, best_offer,
                volume, trades, quantity, listing_count, offer_count
            FROM sfl_resource_prices
            ORDER BY item_key, captured_at DESC
            """
        )
        for row in cur.fetchall():
            state[row["item_key"]] = (
                row["floor"], row["low"], row["high"], row["latest_sale"], row["best_offer"],
                row["volume"], row["trades"], row["quantity"], row["listing_count"], row["offer_count"],
            )
    return state


def fetch_snapshot(api_key: str, date_str: str) -> dict:
    resp = requests.get(
        API_URL,
        params={"type": "marketplaceActivity", "date": date_str},
        headers={"x-api-key": api_key},
        timeout=20,
    )
    resp.raise_for_status()
    return resp.json()


def item_tuple(v: dict) -> tuple:
    return tuple(v.get(f) for f in TRACKED_FIELDS)


def poll_once(conn, api_key: str, item_names: dict, last_state: dict) -> tuple:
    date_str = today_utc_str()
    captured_at = datetime.now(timezone.utc)

    payload = fetch_snapshot(api_key, date_str)
    report = payload.get("data", {}).get("reports", {}).get(date_str)
    if report is None:
        log.warning("В ответе нет данных за %s", date_str)
        return 0, 0

    flower_price = payload.get("data", {}).get("flowerPrice")
    items = report.get("items", {})

    rows = []
    for item_key, v in items.items():
        new_tuple = item_tuple(v)
        if last_state.get(item_key) == new_tuple:
            continue  # ничего не изменилось с прошлого известного значения

        name = item_names.get(item_key, item_key)
        rows.append((
            captured_at, date_str, item_key, name,
            v.get("floor"), v.get("low"), v.get("high"),
            v.get("latestSale"), v.get("bestOffer"),
            v.get("volume"), v.get("trades"), v.get("quantity"),
            v.get("listingCount"), v.get("offerCount"),
            flower_price,
        ))
        last_state[item_key] = new_tuple

    if rows:
        with conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO sfl_resource_prices (
                        captured_at, snapshot_date, item_key, item_name,
                        floor, low, high, latest_sale, best_offer,
                        volume, trades, quantity, listing_count, offer_count,
                        flower_price
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (item_key, captured_at) DO NOTHING
                    """,
                    rows,
                )

    return len(items), len(rows)


def main() -> int:
    api_key = os.environ.get("SFL_API_KEY")
    db_url = os.environ.get("DATABASE_URL")
    interval = int(os.environ.get("POLL_INTERVAL_SECONDS", "300"))

    if not api_key:
        log.error("Нужна переменная окружения SFL_API_KEY")
        return 1
    if not db_url:
        log.error("Нужна переменная окружения DATABASE_URL")
        return 1

    item_names = load_item_names()
    log.info("Известных имён (collectibles-*): %d; остальное — по item_key", len(item_names))
    log.info("Отслеживаю ВСЕ торгуемые предметы из ответа API, интервал опроса %d сек", interval)

    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    ensure_schema(conn)
    log.info("Схема БД готова")

    last_state = load_last_state(conn)
    log.info("Подтянуто последнее состояние по %d предметам из БД", len(last_state))

    while not _shutdown:
        started = time.monotonic()
        try:
            total, changed = poll_once(conn, api_key, item_names, last_state)
            log.info("Опрошено предметов: %d, записано изменений: %d", total, changed)
        except requests.RequestException as e:
            log.error("Ошибка запроса к API: %s", e)
        except psycopg2.Error as e:
            log.error("Ошибка БД: %s", e)
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
            conn = psycopg2.connect(db_url)

        elapsed = time.monotonic() - started
        sleep_for = max(0, interval - elapsed)
        for _ in range(int(sleep_for)):
            if _shutdown:
                break
            time.sleep(1)

    conn.close()
    log.info("Остановлен штатно")
    return 0


if __name__ == "__main__":
    sys.exit(main())
