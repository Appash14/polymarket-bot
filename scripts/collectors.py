# -*- coding: utf-8 -*-
"""Collecteurs de données de marché — indépendants du trading.

Alimentent `data/market_data.db` (base SÉPARÉE de copy99.db : aucun verrou sur la
base des trades, aucun impact sur la latence du bot).

1. poly_ws_collector : **WebSocket CLOB Polymarket** — capture TOUT ce qui bouge sur
   les tokens UP/DOWN de la fenêtre BTC 5m en cours. ~270 events/s (vs 0.4/s en REST
   polling : ~600x plus de données, et SANS marteler l'API REST).
     -> `poly_top`    : meilleur bid/ask à CHAQUE changement (dédupliqué)
     -> `poly_trades` : chaque trade réel (prix, taille, côté taker, tx)
     -> `poly_book`   : snapshots de profondeur complète (throttlés) -> simulation de fill
2. kline_collector : bougies BTC/USDT 5m closes (Binance) -> `klines` (vérité terrain).

Écriture BUFFERISÉE (connexion persistante + WAL + executemany toutes les FLUSH_EVERY s) :
à 270 events/s, ouvrir une connexion par ligne écroulerait tout.

Idempotent : un restart ne casse rien. Les tokens changent à chaque fenêtre (5 min) ->
le WS est re-souscrit automatiquement.
"""

import asyncio
import json
import logging
import sys
import time
from pathlib import Path

import aiohttp
import aiosqlite

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

import polymarket as pm

logger = logging.getLogger("collectors")

DATA_DB     = Path(__file__).parent.parent / "data" / "market_data.db"
POLY_SERIES = "btc-up-or-down-5m"
WINDOW_SECS = 300
CLOB_WS     = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

FLUSH_EVERY   = 1.0     # secondes entre deux flushs en base
BOOK_THROTTLE = 1.0     # 1 snapshot de profondeur max / seconde / token
BOOK_LEVELS   = 15      # niveaux de profondeur conservés de chaque côté
KLINE_POLL    = 60
KLINE_LOOKBACK = 12

_buf_top, _buf_trades, _buf_book = [], [], []
_stats = {"top": 0, "trades": 0, "book": 0, "events": 0}


async def init_data_db():
    async with aiosqlite.connect(DATA_DB) as db:
        await db.execute("PRAGMA journal_mode=WAL")      # lecture possible pendant l'écriture
        await db.execute("""
            CREATE TABLE IF NOT EXISTS poly_top (
                window_start INTEGER, ts REAL, asset_id TEXT, outcome TEXT,
                best_bid REAL, best_ask REAL)""")
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_top ON poly_top(window_start, outcome, ts)")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS poly_trades (
                window_start INTEGER, ts REAL, asset_id TEXT, outcome TEXT,
                price REAL, size REAL, side TEXT, tx_hash TEXT)""")
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_trades ON poly_trades(window_start, ts)")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS poly_book (
                window_start INTEGER, ts REAL, asset_id TEXT, outcome TEXT,
                bids TEXT, asks TEXT)""")
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_book ON poly_book(window_start, ts)")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS klines (
                open_time INTEGER PRIMARY KEY,
                open REAL, high REAL, low REAL, close REAL, volume REAL)""")
        await db.commit()
    logger.info("[collect] base prête : %s (WAL)", DATA_DB.name)


async def _flusher():
    """Vide les buffers en base par lots (connexion persistante)."""
    global _buf_top, _buf_trades, _buf_book
    db = await aiosqlite.connect(DATA_DB)
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA synchronous=NORMAL")        # rapide, sûr en WAL
    try:
        while True:
            await asyncio.sleep(FLUSH_EVERY)
            top, trades, book = _buf_top, _buf_trades, _buf_book
            _buf_top, _buf_trades, _buf_book = [], [], []
            try:
                if top:
                    await db.executemany(
                        "INSERT INTO poly_top VALUES (?,?,?,?,?,?)", top)
                if trades:
                    await db.executemany(
                        "INSERT INTO poly_trades VALUES (?,?,?,?,?,?,?,?)", trades)
                if book:
                    await db.executemany(
                        "INSERT INTO poly_book VALUES (?,?,?,?,?,?)", book)
                if top or trades or book:
                    await db.commit()
            except Exception as e:
                logger.warning("[collect] flush: %s", e)
    finally:
        await db.close()


async def _resolve_tokens():
    """{token_id: 'UP'|'DOWN'} pour la fenêtre courante."""
    out = {}
    for m in await pm.get_series_current_markets(POLY_SERIES) or []:
        o = (m.get("outcome") or "").upper()
        if o in ("UP", "DOWN"):
            out[m["token_id"]] = o
    return out


async def poly_ws_collector():
    """Capture tout le flux CLOB des tokens de la fenêtre courante.
    Re-souscrit à chaque nouvelle fenêtre (les tokens changent toutes les 5 min)."""
    last_top = {}        # asset -> (bid, ask) : déduplication
    last_book = {}       # asset -> ts du dernier snapshot (throttle)
    while True:
        try:
            ws_now = int(time.time() // WINDOW_SECS * WINDOW_SECS)
            tokens = await _resolve_tokens()
            if len(tokens) < 2:
                await asyncio.sleep(2)
                continue
            last_top.clear(); last_book.clear()
            async with aiohttp.ClientSession() as sess:
                async with sess.ws_connect(CLOB_WS, heartbeat=10) as ws:
                    await ws.send_json({"assets_ids": list(tokens), "type": "market"})
                    logger.info("[collect] WS souscrit — fenêtre %d (%d tokens)",
                                ws_now, len(tokens))
                    async for msg in ws:
                        now = time.time()
                        if int(now // WINDOW_SECS * WINDOW_SECS) != ws_now:
                            break                        # fenêtre finie -> re-souscrire
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        try:
                            data = json.loads(msg.data)
                        except Exception:
                            continue
                        for e in (data if isinstance(data, list) else [data]):
                            _handle_event(e, ws_now, now, tokens, last_top, last_book)
        except Exception as e:
            logger.warning("[collect] poly_ws: %s — retry 3s", e)
            await asyncio.sleep(3)


def _handle_event(e, ws, now, tokens, last_top, last_book):
    et = e.get("event_type")
    _stats["events"] += 1

    if et == "price_change":
        # chaque entrée porte DÉJÀ best_bid/best_ask -> pas de reconstruction du carnet
        for pc in e.get("price_changes") or []:
            aid = pc.get("asset_id")
            oc = tokens.get(aid)
            if not oc:
                continue
            try:
                bid = float(pc["best_bid"]); ask = float(pc["best_ask"])
            except (KeyError, TypeError, ValueError):
                continue
            if last_top.get(aid) == (bid, ask):
                continue                                  # top inchangé -> on n'écrit pas
            last_top[aid] = (bid, ask)
            _buf_top.append((ws, round(now, 3), aid, oc, bid, ask))
            _stats["top"] += 1

    elif et == "last_trade_price":
        aid = e.get("asset_id"); oc = tokens.get(aid)
        if not oc:
            return
        try:
            _buf_trades.append((ws, round(now, 3), aid, oc, float(e["price"]),
                                float(e["size"]), e.get("side"), e.get("transaction_hash")))
            _stats["trades"] += 1
        except (KeyError, TypeError, ValueError):
            pass

    elif et == "book":
        # snapshot complet : sert (a) au top-of-book, (b) à la profondeur (throttlée)
        aid = e.get("asset_id"); oc = tokens.get(aid)
        if not oc:
            return
        bids = e.get("bids") or []
        asks = e.get("asks") or []
        # (a) top-of-book — bids triés croissant, asks décroissant : le meilleur est en fin
        try:
            b = max(float(x["price"]) for x in bids)
            a = min(float(x["price"]) for x in asks)
            if last_top.get(aid) != (b, a):
                last_top[aid] = (b, a)
                _buf_top.append((ws, round(now, 3), aid, oc, b, a))
                _stats["top"] += 1
        except (ValueError, KeyError, TypeError):
            pass
        # (b) profondeur — throttlée (volumineux), niveaux les plus proches du mid
        if now - last_book.get(aid, 0) >= BOOK_THROTTLE:
            last_book[aid] = now
            _buf_book.append((ws, round(now, 3), aid, oc,
                              json.dumps(bids[-BOOK_LEVELS:], separators=(",", ":")),
                              json.dumps(asks[-BOOK_LEVELS:], separators=(",", ":"))))
            _stats["book"] += 1


async def kline_collector():
    """Archive les bougies BTC/USDT 5m closes (idempotent, rattrape les trous)."""
    while True:
        try:
            candles = await pm.get_binance_5m_candles(limit=KLINE_LOOKBACK)
            now_ms = time.time() * 1000
            rows = [(int(c[0]), float(c[1]), float(c[2]), float(c[3]), float(c[4]), float(c[5]))
                    for c in candles or []
                    if int(c[0]) + WINDOW_SECS * 1000 <= now_ms]   # closes uniquement
            if rows:
                async with aiosqlite.connect(DATA_DB) as db:
                    await db.executemany(
                        "INSERT OR IGNORE INTO klines VALUES (?,?,?,?,?,?)", rows)
                    await db.commit()
        except Exception as e:
            logger.warning("[collect] kline_collector: %s", e)
        await asyncio.sleep(KLINE_POLL)


async def stats_loop():
    """Log périodique du volume collecté (visibilité + détection de panne)."""
    while True:
        await asyncio.sleep(300)
        try:
            async with aiosqlite.connect(DATA_DB) as db:
                cur = await db.execute(
                    "SELECT COUNT(*), COUNT(DISTINCT window_start) FROM poly_top")
                tops, wins = await cur.fetchone()
                cur = await db.execute("SELECT COUNT(*) FROM poly_trades")
                (tr,) = await cur.fetchone()
                cur = await db.execute("SELECT COUNT(*) FROM poly_book")
                (bk,) = await cur.fetchone()
                cur = await db.execute("SELECT COUNT(*) FROM klines")
                (kl,) = await cur.fetchone()
            mb = DATA_DB.stat().st_size / 1e6 if DATA_DB.exists() else 0
            logger.info("[collect] %d top / %d trades / %d books sur %d fenêtres | "
                        "%d klines | %d events vus | %.1f Mo",
                        tops, tr, bk, wins, kl, _stats["events"], mb)
        except Exception as e:
            logger.debug("[collect] stats: %s", e)
