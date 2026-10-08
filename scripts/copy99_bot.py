"""copy99 — réplique testable de la stratégie du wallet 0x5fcf…75c1
(+$40 911 all-time : achats massifs à 99c sur BTC 5m up/down).

Audit du 12/07/2026 : 2 765 achats en 13 jours, tous à ~0.99, ticket moyen
$176, >100 trades par fenêtre — un whale dont l'edge est la CERTITUDE des
dernières secondes exécutée vite et gros. Sa position courante à -$999.90
montre le revers : à 99c, une perte efface 99 gains.

Adaptation retail (garde-fous non négociables) :
  - Bid MAKER GTC à 0.99 posé UNIQUEMENT quand notre modèle (prix Binance
    temps réel + z-score calibré sur 52k fenêtres) donne l'issue quasi
    verrouillée : z >= Z_MIN et <= MAX_REMAIN secondes restantes.
  - Retrait immédiat du bid si z redescend sous Z_CANCEL (le vendeur qui
    nous remplirait en sait plus que nous).
  - 50% du capital par bet (choix de Tom), 1 position par fenêtre.
  - ARRÊT TOTAL à la première perte : à ces cotes le breakeven est 99%,
    une seule perte est un signal statistique fort que l'edge n'est pas là.
  - Notifications Telegram en envoi seul (PAS de getUpdates : le token est
    partagé avec edge_trader, un seul consommateur possible).

Lancer :  python scripts/copy99_bot.py
"""

import asyncio
import json
import logging
import sys
import time
from math import erf, floor, sqrt
from pathlib import Path

import aiohttp
import aiosqlite

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

import polymarket as pm
from config import PRIVATE_KEY, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
from single_instance import acquire_lock
from telegram_ui import TelegramUI

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    # FICHIER UNIQUEMENT : le terminal affiche le dashboard live (terminal_ui.py),
    # un StreamHandler le corromprait à chaque log. Tout est dans data/copy99.log.
    handlers=[logging.FileHandler(Path(__file__).parent.parent / "data" / "copy99.log",
                                  encoding="utf-8")],
)
logger = logging.getLogger("copy99")

DB_PATH = Path(__file__).parent.parent / "data" / "copy99.db"
POLY_SERIES = "btc-up-or-down-5m"
BINANCE_WS = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"
WINDOW_SECS = 300

# ── Paramètres ────────────────────────────────────────────────────────────────
BID_PRICE     = 0.99    # notre bid maker
STAKE_PCT     = 0.9975  # ~100% du capital (choix de Tom 13/07). 0.9975 (pas 1.0)
                        # + coussin $0.05 dans le sizing = évite le rejet "dust"
                        # (le solde renvoyé est parfois une poussière > spendable réel)
# Copie fidèle du whale (audit timing du 12/07 : 68% de ses fills entre la
# 2e et la 4e minute, prix fixe 0.99 = bid maker au repos, déclencheur = le
# marché lui-même à ~99c). Il absorbe les cash-outs anticipés des gagnants —
# flux non informé par construction.
ASK_MIN       = 0.985   # ne poste que si l'ask du côté gagnant cote déjà >= 98.5c
                        # (le signal du whale : le consensus du marché)
Z_MIN         = 3.5     # relevé de 2.5 à 3.5 (choix Tom 13/07) : plus sélectif,
                        # ~99.98% théorique. Le #59 (z=3.39) aurait été skippé.
Z_CANCEL      = 2.0     # retrait du bid si z retombe sous ce niveau
ASK_CANCEL    = 0.97    # retrait si le marché doute à nouveau (ask < 97c)
MAX_REMAIN    = 125     # on n'entre que sous 125s restants (choix de Tom 13/07,
                        # ~p75 whale=118s) — early entries écartées + stop-loss
MIN_REMAIN    = 8       # plus rien sous 8s (résolution/latence)
KILL_BALANCE  = 0.0     # plancher désactivé (choix Tom 13/07 — laisse jouer la cartouche)
STOP_ON_LOSS  = True    # ARRÊT TOTAL à la première perte NON écrêtée (cf. stop-loss)
VOL_REFRESH   = 1800

# ── Stop-loss protecteur (ajouté 13/07) ───────────────────────────────────────
# Si BTC repasse du mauvais côté du strike (notre token plonge vers ~0.50), on
# revend en FOK pour écrêter la perte au lieu de la subir pleine. L'analyse des 59
# trades montre que 0 gagnant n'a jamais croisé le strike → quasi aucun faux
# déclenchement. Vente REMPLIE → perte écrêtée, on CONTINUE. Vente ÉCHOUÉE (carnet
# vide) → on laisse expirer (perte pleine) et STOP_ON_LOSS s'applique.
PROTECT_EXIT     = True
EXIT_CONFIRM     = 2          # ticks (~s) sous le strike avant de vendre (anti-mèche)
EXIT_MIN_REMAIN  = 10         # sous 10s on ne vend plus, on laisse résoudre
EXIT_COOLDOWN    = 6          # s entre deux tentatives si la 1re vente échoue
EXIT_FLOORS      = [0.48, 0.43, 0.38]   # limites FOK décroissantes (maximise le fill)

# ── Stratégie active ──────────────────────────────────────────────────────────
STRATEGY = "reversal"        # "maker" (bid 0.99) | "reversal" (breakout momentum)

# ── Reversal breakout / momentum (ajouté 13/07) ───────────────────────────────
# Un token vu sous REV_LOW puis dont l'ASK franchit REV_TRIGGER -> ACHAT taker
# (FOK), hold jusqu'à expiry. UP ET DOWN. 20% du capital, SANS stop-loss (Tom).
# Backtest 6 mois : win ~72% pour une entrée réelle ~0.56 -> +15% net. TOUT l'edge
# dépend du FILL réel (rentrer près de 0.56 pendant la montée) -> test LIVE.
REV_STAKE_PCT = 0.20
REV_LOW       = 0.40         # le token doit avoir été vu sous ce prix (côté perdant)
REV_TRIGGER   = 0.51         # achat quand l'ask franchit ce niveau (retournement) —
                             # 0.51 (choix Tom) : détection + tôt = meilleur fill avant gap
REV_MAX_ENTRY = 0.58         # ne JAMAIS payer au-dessus (si l'ask a gappé -> skip, edge parti).
                             # 0.65 -> 0.58 le 16/07 : sur 375 trades, l'edge s'évapore au-dessus
                             # de ~0.56 (payer 0.58 exige 58% de WR, on n'en a que 59.5%).
                             # Cf. docs/ANALYSE_375_TRADES_2026-07-16.md
REV_MIN_REMAIN= 8            # pas d'entrée sous 8s (exécution/résolution)
REV_POLL      = 1.0          # cadence de surveillance des asks (s)
REV_TP        = 0.99         # vente auto (TP) dès que le bid atteint ce prix : libère le
                             # capital + verrouille le gain avant un retournement tardif
REV_MIN_SELL_SHARES = 5      # Polymarket rejette un ordre LIMITE < 5 shares ("minimum: 5").
                             # Avec le sizing à paliers ($1-3), on a souvent 2-4 shares -> pas
                             # d'ordre 0.99 possible : on HOLD jusqu'à expiry (résolution OK).
REV_BASELINE  = 9.66         # solde de départ de la stratégie reversal (référence du PnL)
REV_STEP      = 10.0         # mise = $1 par tranche de REV_STEP $ de capital (min $1).
                             # ex: $30->$3, $47->$4, $23->$2, <$10->$1 (~10% par paliers)

# ── Réglages MODIFIABLES en live via Telegram (persistés sur disque) ──────────
# Les constantes REV_* ci-dessus servent de valeurs par défaut ; l'utilisateur peut
# les changer depuis le menu ⚙️ Réglages sans toucher au code ni perdre au restart.
SETTINGS_PATH = DB_PATH.parent / "reversal_settings.json"
DEFAULT_SETTINGS = {
    "bet_mode":  "pct",         # "pct" (% du solde) ou "fixed" ($ fixe)
    "bet_value": 10.0,          # 10 => 10% du solde, ou $10 si fixed
    "low":       REV_LOW,       # 0.40  arme un side quand ask <= low
    "trigger":   REV_TRIGGER,   # 0.51  achat quand ask >= trigger
    "tmin":      REV_MIN_REMAIN,# 8     temps restant minimum (s)
    "tmax":      240,           # 240   temps restant maximum (s)
    "tp_mode":   "hold",        # "sell99" = ordre limite de vente à 0.99 (si >=5 shares)
                                # "hold"   = pas de TP, on attend la résolution
}
def load_settings():
    s = dict(DEFAULT_SETTINGS)
    try:
        with open(SETTINGS_PATH, encoding="utf-8") as f:
            s.update({k: v for k, v in json.load(f).items() if k in DEFAULT_SETTINGS})
    except Exception:
        pass
    return s
def save_settings(s):
    try:
        with open(SETTINGS_PATH, "w", encoding="utf-8") as f:
            json.dump(s, f)
    except Exception as e:
        logger.warning("save_settings: %s", e)

# Spectateurs Telegram (LECTURE SEULE) : reçoivent le dashboard, aucune commande possible.
# Ils s'enregistrent en écrivant une fois au bot (reconnus par username).
# Vide par défaut : seul le propriétaire (TELEGRAM_CHAT_ID) voit le bot.
VIEWER_USERNAMES = []

# (Monitoring du whale retiré le 16/07 : la stratégie maker copiée est abandonnée,
#  la reversal n'en dépend pas. Code + whale.db dans archive/2026-07-16_cleanup/.)


def phi(x: float) -> float:
    return 0.5 * (1 + erf(x / sqrt(2)))


_UI: "TelegramUI | None" = None
_BOT = None      # instance courante — exposée pour le dashboard terminal (bot.py)


async def notify(text: str, ttl: float = 6 * 3600):
    if _UI is not None:
        await _UI.notify(text, ttl=ttl)
        return
    if not TELEGRAM_BOT_TOKEN:
        return
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                         json={"chat_id": TELEGRAM_CHAT_ID, "text": text},
                         timeout=aiohttp.ClientTimeout(total=8))
    except Exception as e:
        logger.warning("telegram: %s", e)


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                window_start INTEGER NOT NULL UNIQUE,
                direction TEXT, token_id TEXT,
                entry_price REAL, shares REAL, size_usdc REAL,
                z_at_fill REAL, opened_at REAL,
                resolved INTEGER DEFAULT 0, won INTEGER, pnl_usdc REAL
            );
            CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT);
        """)
        await db.commit()


class Copy99:
    def __init__(self):
        self.api_key = self.api_secret = self.api_passphrase = ""
        self.window_start = 0
        self.up_token = self.down_token = None
        self.btc_open = None
        self.btc_price = None
        self.vol1m = None
        self.vol_ts = 0.0
        self.order = None        # {order_id, token_id, direction, shares, window_start}
        self.position = None     # trade dict après fill
        self.stopped = False
        self.paused = False
        self._dash_bal = 0.0
        self._dash_bal_ts = 0.0
        self._exiting = False    # garde anti double-vente (stop-loss)
        self._uw = 0             # compteur de ticks sous le strike
        self._exit_cd = 0.0      # cooldown entre tentatives de vente
        self._rev = {}           # reversal : token -> {"low": bool} (état par fenêtre)
        self.rev_baseline = None  # équité de départ figée -> PnL reversal = équité - baseline
        self.rev_start_id = 0     # id de trade à partir duquel on compte le WR reversal
        self.settings = load_settings()   # réglages live (bet size + conditions)

    async def load_creds(self):
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute("SELECT value FROM state WHERE key='api_creds'")
            row = await cur.fetchone()
        if row:
            d = json.loads(row[0])
        else:
            d = await pm.create_api_creds(PRIVATE_KEY)
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("INSERT OR REPLACE INTO state VALUES ('api_creds',?)",
                                 (json.dumps(d),))
                await db.commit()
        self.api_key, self.api_secret, self.api_passphrase = (
            d["api_key"], d["api_secret"], d["api_passphrase"])

    async def balance(self):
        return await pm.get_clob_balance(PRIVATE_KEY, self.api_key,
                                         self.api_secret, self.api_passphrase)

    async def reload_position(self):
        """Recharge une position ouverte (resolved=0) après un redémarrage pour ne
        pas l'abandonner. Strike non stocké -> résolution via le book (poly)."""
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute(
                "SELECT id, window_start, direction, token_id, entry_price, shares, size_usdc"
                " FROM trades WHERE resolved=0 ORDER BY id DESC LIMIT 1")
            row = await cur.fetchone()
        if not row:
            return
        tid, ws, direction, token, entry, shares, size = row
        self.position = dict(id=tid, token_id=token, direction=direction, shares=shares,
                             matched=shares, size=size, window_start=ws, strike=None,
                             z=entry, entry=entry, sell_order=None, _sold=0.0,
                             _hold=(shares < REV_MIN_SELL_SHARES
                                    or self.settings.get("tp_mode", "hold") != "sell99"))
        logger.info("position rechargée après redémarrage (#%d %s) — résolution via book", tid, direction)

    # ── Fenêtres & données ────────────────────────────────────────────────────

    async def refresh_window(self):
        """Auto-réparant : si un appel REST échoue au changement de fenêtre,
        on réessaie chaque seconde tant que la fenêtre est incomplète (bug du
        12/07 : un échec ponctuel laissait le bot aveugle 5 minutes —
        '₿ en attente de données' sur le dashboard)."""
        now = int(time.time())
        ws = now // WINDOW_SECS * WINDOW_SECS
        if ws != self.window_start:
            if self.order:
                await self.cancel_bid("nouvelle fenêtre")
            self.window_start = ws
            # open immédiat depuis le flux WS (aucune dépendance REST),
            # affiné juste après par la kline Binance officielle
            self.btc_open = self.btc_price
            self._open_refined = False
            self.up_token = self.down_token = None
            self._rev = {}                     # reset état reversal à chaque fenêtre
        if not getattr(self, "_open_refined", True):
            try:
                o = await pm.get_btc_5m_open()
                if o:
                    self.btc_open = o
                    self._open_refined = True
            except Exception:
                pass    # retenté au prochain tick
        if not self.up_token or not self.down_token:
            try:
                tokens = await pm.get_series_current_markets(POLY_SERIES)
            except Exception:
                tokens = []
            for t in tokens:
                o = (t.get("outcome") or "").upper()
                if o == "UP":
                    self.up_token = t["token_id"]
                elif o == "DOWN":
                    self.down_token = t["token_id"]
            if self.up_token and self.down_token:
                logger.info("Fenêtre %d | open=%.2f | UP=%s DOWN=%s", ws,
                            self.btc_open or 0, self.up_token[:12], self.down_token[:12])

    async def window_loop(self):
        while True:
            try:
                await self.refresh_window()
            except Exception as e:
                logger.warning("refresh_window: %s", e)
            await asyncio.sleep(1)

    async def binance_loop(self):
        while True:
            try:
                async with aiohttp.ClientSession() as sess:
                    async with sess.ws_connect(BINANCE_WS, heartbeat=20) as ws:
                        logger.info("Binance WS connecté")
                        async for msg in ws:
                            if msg.type != aiohttp.WSMsgType.TEXT:
                                continue
                            self.btc_price = float(json.loads(msg.data)["p"])
                            if STRATEGY == "maker":
                                await self.evaluate()
            except Exception as e:
                logger.warning("binance ws: %s — retry 3s", e)
                await asyncio.sleep(3)

    async def vol_loop(self):
        while True:
            try:
                async with aiohttp.ClientSession() as sess:
                    async with sess.get(
                        "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m&limit=1440",
                        timeout=aiohttp.ClientTimeout(total=20)) as r:
                        kl = await r.json()
                closes = [float(k[4]) for k in kl]
                rets = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
                mean = sum(rets) / len(rets)
                self.vol1m = sqrt(sum((x - mean) ** 2 for x in rets) / (len(rets) - 1))
                self.vol_ts = time.time()
                logger.info("vol 1m ($): %.2f", self.vol1m)
            except Exception as e:
                logger.warning("vol_loop: %s", e)
            await asyncio.sleep(VOL_REFRESH)

    # ── Signal & ordres ───────────────────────────────────────────────────────

    def z_now(self):
        if None in (self.btc_price, self.btc_open, self.vol1m) or self.vol1m <= 0:
            return None
        remain = self.window_start + WINDOW_SECS - time.time()
        if remain <= 0:
            return None
        return (self.btc_price - self.btc_open) / (self.vol1m * sqrt(max(remain, 1) / 60))

    # ── Dashboard & actions ───────────────────────────────────────────────────

    async def build_dashboard(self) -> str:
        now = time.time()
        if now - self._dash_bal_ts > 25:
            try:
                self._dash_bal = await self.balance()
                self._dash_bal_ts = now
            except Exception:
                pass
        # ── Dashboard REVERSAL (stats de CETTE stratégie uniquement) ───────────
        if STRATEGY == "reversal":
            remain = max(0, int(self.window_start + WINDOW_SECS - now))
            state = "🛑 STOPPÉ" if self.stopped else \
                    "⏸ PAUSE" if self.paused else "🟢 LIVE"
            # bid de la position ouverte (réutilisé pour le latent + le bloc trade)
            pos_bid = None
            if self.position:
                try:
                    pos_bid = await pm.get_best_price(self.position["token_id"], "BUY")
                except Exception:
                    pos_bid = None
            try:
                up_ask = await pm.get_best_price(self.up_token, "SELL") if self.up_token else None
            except Exception:
                up_ask = None
            try:
                dn_ask = await pm.get_best_price(self.down_token, "SELL") if self.down_token else None
            except Exception:
                dn_ask = None
            async with aiosqlite.connect(DB_PATH) as db:
                cur = await db.execute(
                    "SELECT COUNT(*), COALESCE(SUM(pnl_usdc),0), COALESCE(SUM(won),0),"
                    # fill RÉEL = $ dépensés / shares reçues (= breakeven WR à atteindre)
                    " COALESCE(AVG(CASE WHEN shares>0 THEN size_usdc/shares END),0)"
                    " FROM trades WHERE resolved=1 AND entry_price < 0.9")
                n, tot, w, avg_fill = await cur.fetchone()
            # ARGENT RÉEL : cash + valeur de la position ouverte (jamais > total)
            pos_val = (self.position["matched"] * pos_bid) if (self.position and pos_bid is not None) \
                      else (self.position["size"] if self.position else 0.0)
            money = self._dash_bal + pos_val
            pnl = money - self.rev_baseline if self.rev_baseline else 0.0
            pct = (pnl / self.rev_baseline * 100) if self.rev_baseline else 0.0
            def _px(v):
                return f"{v:.2f}" if v is not None else "–"
            lines = [
                f"{state}",
                f"💰 ${money:.2f}  |  PnL {pnl:+.2f}$ ({pct:+.2f}%)",
                f"📊 WR : {(w/n*100) if n else 0:.2f}%  {w}W {n-w}L  ({n} trades)",
                f"🎯 Fill moyen : ~{avg_fill:.2f}  (breakeven {avg_fill*100:.2f}%)",
                "",
            ]
            if self.btc_price and self.btc_open:
                diff = self.btc_price - self.btc_open
                op = f"{self.btc_open:,.0f}".replace(",", "'")
                nw = f"{self.btc_price:,.0f}".replace(",", "'")
                tri = "▲" if diff >= 0 else "▼"
                dtxt = f"{diff:+.2f}" if abs(diff) < 1 else f"{diff:+.0f}"   # 2 déc. seulement si < 1$
                lines.append(f"₿ {op}$   →   {nw}$    {dtxt}$ {tri}")
            else:
                lines.append("₿ …")
            lines.append(f"UP {_px(up_ask)}   |   DOWN {_px(dn_ask)}")
            lines.append(f"{remain//60}m{remain%60:02d}s")
            lines.append("")
            lines.append("📈 Position :")
            if self.position and now > self.position["window_start"] + WINDOW_SECS:
                p = self.position
                lines.append(f" {p['matched']:.1f} {p['direction']} @ {p.get('entry',0):.2f} — en résolution…")
            elif self.position:
                p = self.position
                cost = p["size"]
                ppct = ((pos_val - cost) / cost * 100) if cost else 0.0
                lines.append(f" {p['matched']:.1f} {p['direction']} @ {p.get('entry',0):.2f} — ${cost:.2f}")
                lines.append(f" Live value : ${pos_val:.2f} ({ppct:+.0f}%)")
            else:
                lines.append(" —")
            lines.append("")
            lines.append(f"🕐 {time.strftime('%H:%M:%S')}")
            return "\n".join(lines)
        # ── Dashboard MAKER (copy99 0.99) ──────────────────────────────────────
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute("SELECT COUNT(*), COALESCE(SUM(pnl_usdc),0),"
                                   " COALESCE(SUM(won),0) FROM trades WHERE resolved=1")
            n, tot, w = await cur.fetchone()
        base = self._dash_bal - tot          # capital avant P&L (proxy)
        pct = (tot / base * 100) if base > 0 else 0.0
        z = self.z_now()
        remain = max(0, int(self.window_start + WINDOW_SECS - now))
        state = "🛑 STOPPÉ (perte)" if self.stopped else \
                "⏸ PAUSE" if self.paused else "🟢 LIVE"
        lines = [
            f"🧪 COPY99 {state} — maker 99c",
            f"💰 ${self._dash_bal:.2f}  |  PnL {tot:+.2f}$ ({pct:+.1f}%)",
            f"📊 WR : {(w/n*100) if n else 0:.0f}%  {w}W {n-w}L",
            "",
            f"₿ ${self.btc_price:,.0f} | open ${self.btc_open:,.0f} | "
            f"z={z:+.2f} | {remain//60}m{remain%60:02d}s"
            if None not in (self.btc_price, self.btc_open) and z is not None
            else "₿ en attente de données…",
        ]
        if self.order:
            o = self.order
            lines.append(f"⏳ Bid en attente : {o['direction']} {o['shares']:.1f} sh "
                         f"@ {BID_PRICE} (z au post {o['z']:+.2f})")
        elif self.position:
            p = self.position
            lines.append(f"📈 Position : {p['direction']} {p['matched']:.1f} sh @ {BID_PRICE} "
                         f"— résolution imminente")
        else:
            lines.append(f"🔍 Surveillance (ask ≥ {ASK_MIN} + |z| ≥ {Z_MIN}, "
                         f"3 dernières min)")
        lines.append(f"⚙️ mise {int(STAKE_PCT*100)}% | 🛡️ stop-loss sous strike armé")
        lines.append(f"🕐 {time.strftime('%H:%M:%S')}")
        return "\n".join(lines)

    async def on_ui_action(self, action: str):
        if action == "pause":
            self.paused = True
            if self.order:
                await self.cancel_bid("pause demandée")
            return "⏸ copy99 en pause."
        if action == "resume":
            if self.stopped:
                return "🛑 Stoppé après perte — relance manuelle du process requise."
            self.paused = False
            return "▶️ copy99 repris."
        if action in ("refresh", "status", "trades"):
            self._dash_bal_ts = 0
            if _UI is not None:
                _UI._dash_dirty_pos = True
            return "Dashboard actualisé."
        return None

    def _rev_stake(self, bal):
        """Mise selon les réglages live. Toujours >= $1."""
        s = self.settings
        if s["bet_mode"] == "fixed":
            stake = round(float(s["bet_value"]), 2)        # $ exact
        else:
            stake = floor(bal * float(s["bet_value"]) / 100)  # % du solde, arrondi $ entier (anti-dust)
        return max(1.0, float(stake))

    async def apply_setting(self, key, raw):
        """Applique une valeur saisie via le menu Telegram. Retourne un message de confirmation."""
        s = self.settings
        try:
            if key == "bet_mode":
                s["bet_mode"] = "fixed" if raw == "fixed" else "pct"
                save_settings(s)
                unit = "en $ (ex: 3)" if s["bet_mode"] == "fixed" else "en % (ex: 10)"
                return f"Mode mise : {s['bet_mode'].upper()}. Écris maintenant la valeur {unit}."
            if key == "tp_mode":
                s["tp_mode"] = "sell99" if raw == "sell99" else "hold"
                save_settings(s)
                # appliquer immédiatement à une position déjà ouverte
                if self.position:
                    self.position["_hold"] = (s["tp_mode"] != "sell99")
                return ("✅ Take profit : ordre de vente à 0.99 (si ≥5 shares)."
                        if s["tp_mode"] == "sell99"
                        else "✅ Take profit OFF : on attend la résolution.")
            v = float(str(raw).replace(",", ".").replace("$", "").replace("%", "").strip())
            if key.startswith("bet_value"):
                s["bet_value"] = max(1.0, v) if s["bet_mode"] == "fixed" else max(0.1, v)
                shown = f"${s['bet_value']:.2f}" if s["bet_mode"] == "fixed" \
                    else f"{s['bet_value']:.2f}% du solde"
                msg = f"Mise : {shown} (min $1)."
            elif key == "low":
                s["low"] = max(0.01, min(0.98, v)); msg = f"Armement ≤ {s['low']:.2f}."
            elif key == "trigger":
                s["trigger"] = max(0.02, min(0.99, v)); msg = f"Achat ≥ {s['trigger']:.2f}."
            elif key == "tmin":
                s["tmin"] = int(max(1, min(v, s["tmax"]))); msg = f"Temps restant min : {s['tmin']}s."
            elif key == "tmax":
                s["tmax"] = int(max(s["tmin"], min(v, WINDOW_SECS))); msg = f"Temps restant max : {s['tmax']}s."
            else:
                return None
            save_settings(s)
            return "✅ " + msg
        except Exception:
            return "❌ Valeur invalide, réessaie."

    async def _fresh_ask(self, token: str):
        """Ask CLOB du token, caché 3s (évite de marteler l'API à chaque tick)."""
        now = time.time()
        cache = getattr(self, "_ask_cache", {})
        if token in cache and now - cache[token][1] < 3:
            return cache[token][0]
        try:
            ask = await pm.get_best_price(token, "SELL")
        except Exception:
            ask = None
        cache[token] = (ask, now)
        self._ask_cache = cache
        return ask

    async def evaluate(self):
        if self.stopped or self.paused or self.position is not None:
            return
        z = self.z_now()
        if z is None:
            return
        remain = self.window_start + WINDOW_SECS - time.time()
        # retrait si le bid en place n'est plus couvert par le signal
        if self.order:
            side_z = z if self.order["direction"] == "UP" else -z
            ask = await self._fresh_ask(self.order["token_id"])
            if side_z < Z_CANCEL or remain < MIN_REMAIN or (ask is not None and ask < ASK_CANCEL):
                await self.cancel_bid(f"side_z={side_z:.2f} ask={ask} remain={remain:.0f}s")
            return
        if not (MIN_REMAIN <= remain <= MAX_REMAIN):
            return
        direction = "UP" if z > 0 else "DOWN"
        if abs(z) < Z_MIN:
            return
        token = self.up_token if direction == "UP" else self.down_token
        if not token:
            return
        # signal du whale : le marché lui-même doit déjà coter la quasi-certitude
        ask = await self._fresh_ask(token)
        if ask is None or ask < ASK_MIN:
            return
        # une seule tentative par fenêtre (garde DB)
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute("SELECT id FROM trades WHERE window_start=?",
                                   (self.window_start,))
            if await cur.fetchone():
                return
        bal = await self.balance()
        if bal < KILL_BALANCE:
            self.stopped = True
            await notify(f"🛑 copy99 : solde ${bal:.2f} < ${KILL_BALANCE} — arrêt.")
            return
        # coussin de $0.05 : le solde renvoyé peut être une poussière au-dessus du
        # spendable réel -> à 100% l'ordre se faisait rejeter (balance dust). floor
        # (pas round) pour garantir coût = shares*prix <= solde.
        usable = max(bal * STAKE_PCT - 0.05, 0.0)
        shares = max(floor(usable / BID_PRICE * 100) / 100, 5.0)
        logger.info("SIGNAL %s z=%.2f remain=%.0fs → bid maker %.2f × %.1f sh ($%.2f)",
                    direction, z, remain, BID_PRICE, shares, shares * BID_PRICE)
        try:
            res = await pm.place_limit_order(
                private_key=PRIVATE_KEY, api_key=self.api_key,
                api_secret=self.api_secret, api_passphrase=self.api_passphrase,
                token_id=token, side="BUY", price=BID_PRICE, shares=shares)
        except Exception as e:
            logger.error("place_limit_order: %s", e)
            return
        oid = res.get("orderID") if isinstance(res, dict) else None
        if not oid:
            logger.warning("ordre refusé: %s", res)
            return
        self.order = dict(order_id=oid, token_id=token, direction=direction,
                          shares=shares, window_start=self.window_start,
                          z=z, ts=time.time())

    # ── Stratégie REVERSAL (breakout momentum, UP + DOWN) ──────────────────────
    async def rev_loop(self):
        """Surveille les asks UP/DOWN (entrée) ou le bid (TP 0.99) selon l'état."""
        while True:
            await asyncio.sleep(REV_POLL)
            if STRATEGY != "reversal":
                continue
            try:
                if self.position is not None:
                    await self.manage_sell()       # position ouverte -> ordre vente 0.99 + fill
                else:
                    await self.reversal_eval()     # pas de position -> guette un breakout
            except Exception as e:
                logger.debug("rev_loop: %s", e)

    async def manage_sell(self):
        """Assure un ordre LIMITE GTC de vente à 0.99 pour 100% des shares détenues,
        et détecte son remplissage complet -> verrouille le gain + libère le capital.
        (Ordre au repos = se remplit au fil des acheteurs, pas de tout-ou-rien du FOK.)"""
        p = self.position
        if not p:
            return
        if p.get("_hold"):
            return                          # TP off ou < 5 sh : rien à gérer, hold jusqu'à expiry
        # 1) poser l'ordre de vente 0.99 s'il n'existe pas encore (ex: après un reload)
        if not p.get("sell_order"):
            if self.settings.get("tp_mode", "hold") != "sell99":   # TP désactivé
                p["_hold"] = True
                return
            if p.get("matched", 0) < REV_MIN_SELL_SHARES:  # trop peu de shares pour un ordre limite
                p["_hold"] = True
                return
            if time.time() - p.get("_sell_try", 0) < 8:   # cooldown anti-spin
                return
            p["_sell_try"] = time.time()
            try:
                res = await pm.place_limit_order(
                    private_key=PRIVATE_KEY, api_key=self.api_key, api_secret=self.api_secret,
                    api_passphrase=self.api_passphrase, token_id=p["token_id"],
                    side="SELL", price=REV_TP, shares=p["matched"])
                oid = res.get("orderID") if isinstance(res, dict) else None
                if oid:
                    p["sell_order"] = oid
                    logger.info("REVERSAL ordre vente 0.99 posé (%s %.1f sh)", p["direction"], p["matched"])
            except Exception as e:
                logger.debug("place sell 0.99: %s", e)
            return
        # 2) détecter le remplissage complet -> clôture
        try:
            st = await pm.get_order_status(PRIVATE_KEY, self.api_key, self.api_secret,
                                           self.api_passphrase, p["sell_order"])
        except Exception:
            return
        if not isinstance(st, dict):
            return
        matched = float(st.get("size_matched") or 0)
        status = (st.get("status") or "").upper()
        p["_sold"] = matched
        if status in ("MATCHED", "FILLED") or matched >= p["matched"] - 0.02:  # ordre vente bouclé
            pnl = round(matched * REV_TP - p["size"], 2)
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("UPDATE trades SET resolved=1, won=1, pnl_usdc=? WHERE id=?",
                                 (pnl, p["id"]))
                await db.commit()
            self.position = None
            await notify(f"💚 REVERSAL {p['direction']} vendu @ 0.99 (100%) — {pnl:+.2f}$")
            logger.info("REVERSAL vente 0.99 complète pnl=%+.2f", pnl)

    async def reversal_eval(self):
        if self.stopped or self.paused or self.position is not None:
            return
        cfg = self.settings
        remain = self.window_start + WINDOW_SECS - time.time()
        if not (cfg["tmin"] <= remain <= cfg["tmax"]):   # min ET max (fenêtre d'entrée)
            return
        for token, direction in ((self.up_token, "UP"), (self.down_token, "DOWN")):
            if not token:
                continue
            try:
                ask = await pm.get_best_price(token, "SELL")   # prix d'ACHAT (ask)
            except Exception:
                ask = None
            if ask is None:
                continue
            st = self._rev.setdefault(token, {"low": False})
            st["ask"] = ask                            # pour l'affichage dashboard
            if ask <= cfg["low"]:
                st["low"] = True                       # a bien été côté perdant
            elif st["low"] and ask >= cfg["trigger"]:
                if ask > REV_MAX_ENTRY:
                    logger.info("reversal %s ask %.2f > max %.2f -> skip (gappé)",
                                direction, ask, REV_MAX_ENTRY)
                    st["low"] = False                  # ré-armer seulement si re-<0.40
                    continue
                await self.rev_buy(token, direction, ask, remain)
                return

    async def rev_buy(self, token, direction, ask, remain):
        # une seule entrée par fenêtre
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute("SELECT id FROM trades WHERE window_start=?",
                                   (self.window_start,))
            if await cur.fetchone():
                return
        bal = await self.balance()
        if bal < KILL_BALANCE:
            self.stopped = True
            await notify(f"🛑 reversal : solde ${bal:.2f} < ${KILL_BALANCE} — arrêt.")
            return
        stake = self._rev_stake(bal)
        if bal < stake:
            return
        limit = REV_MAX_ENTRY    # FOK peut remplir sur tout le carnet jusqu'à ce plafond
                                 # (on remplit au meilleur prix dispo, sans payer > REV_MAX_ENTRY)
        logger.info("REVERSAL %s ask=%.3f remain=%.0fs -> BUY taker $%.2f (limit %.2f)",
                    direction, ask, remain, stake, limit)
        try:
            res = await pm.place_order(
                private_key=PRIVATE_KEY, api_key=self.api_key,
                api_secret=self.api_secret, api_passphrase=self.api_passphrase,
                token_id=token, side="BUY", size_usdc=stake, limit_price=limit)
        except Exception as e:
            logger.error("reversal place_order: %s", e)
            return
        status = (res.get("status") or "").upper() if isinstance(res, dict) else ""
        if status not in ("MATCHED", "FILLED"):
            logger.warning("reversal buy non rempli: %s", res)
            return
        entry = ask
        # Quantité RÉELLEMENT remplie (autoritative) — PAS l'estimation stake/entry, qui laisse
        # des poussières quand le prix a bougé entre la lecture de l'ask et le fill. On lit le
        # fill de l'ordre d'achat après un court délai (laisse le solde de parts se propager,
        # sinon l'ordre de vente est capé sous le total). Fallback = estimation si indispo.
        buy_oid = res.get("orderID") if isinstance(res, dict) else None
        actual = None
        if buy_oid:
            await asyncio.sleep(2.0)
            try:
                bst = await pm.get_order_status(PRIVATE_KEY, self.api_key, self.api_secret,
                                                self.api_passphrase, buy_oid)
                if isinstance(bst, dict):
                    actual = float(bst.get("size_matched") or 0) or None
            except Exception as e:
                logger.debug("lecture fill achat: %s", e)
        shares = (floor(actual * 100) / 100) if actual else round(stake / entry, 2)  # floor: anti-oversell
        async with aiosqlite.connect(DB_PATH) as db:
            try:
                cur = await db.execute(
                    "INSERT INTO trades (window_start, direction, token_id,"
                    " entry_price, shares, size_usdc, z_at_fill, opened_at)"
                    " VALUES (?,?,?,?,?,?,?,?)",
                    (self.window_start, direction, token, entry, shares, stake,
                     entry, time.time()))
                await db.commit()
                trade_id = cur.lastrowid
            except aiosqlite.IntegrityError:
                return
        self.position = dict(id=trade_id, token_id=token, direction=direction,
                             shares=shares, matched=shares, size=stake,
                             window_start=self.window_start, strike=self.btc_open,
                             z=entry, entry=entry, sell_order=None, _sold=0.0)
        # ordre LIMITE GTC de vente à 0.99 pour 100% des shares, DIRECT après l'achat.
        # Seulement si tp_mode == "sell99" ET >= 5 shares (min Polymarket) ; sinon HOLD.
        if self.settings.get("tp_mode", "hold") != "sell99":
            self.position["_hold"] = True          # TP désactivé -> on attend la résolution
        elif shares >= REV_MIN_SELL_SHARES:
            try:
                sres = await pm.place_limit_order(
                    private_key=PRIVATE_KEY, api_key=self.api_key, api_secret=self.api_secret,
                    api_passphrase=self.api_passphrase, token_id=token, side="SELL",
                    price=REV_TP, shares=shares)
                self.position["sell_order"] = sres.get("orderID") if isinstance(sres, dict) else None
            except Exception as e:
                logger.warning("reversal ordre vente 0.99: %s", e)
        else:
            self.position["_hold"] = True      # < 5 sh : hold jusqu'à expiry (résolution via book)
        await notify(f"🟢 REVERSAL {direction} — acheté {shares:.1f} sh @ ~{entry:.2f} (${stake:.2f})")
        logger.info("REVERSAL FILL %s %.1f sh @ %.3f ($%.2f) + vente %.2f posée",
                    direction, shares, entry, stake, REV_TP)

    async def cancel_bid(self, reason=""):
        o, self.order = self.order, None
        if not o:
            return
        try:
            await pm.cancel_order(PRIVATE_KEY, self.api_key, self.api_secret,
                                  self.api_passphrase, o["order_id"])
        except Exception:
            pass
        logger.info("bid retiré (%s)", reason)

    async def fill_loop(self):
        while True:
            await asyncio.sleep(1.5)
            o = self.order
            if not o or self.stopped:
                continue
            try:
                st = await pm.get_order_status(PRIVATE_KEY, self.api_key,
                                               self.api_secret, self.api_passphrase,
                                               o["order_id"])
            except Exception:
                continue
            if not isinstance(st, dict):
                continue
            matched = float(st.get("size_matched") or 0)
            status = (st.get("status") or "").upper()
            if matched > 0:
                size = round(matched * BID_PRICE, 2)
                async with aiosqlite.connect(DB_PATH) as db:
                    try:
                        cur = await db.execute(
                            "INSERT INTO trades (window_start, direction, token_id,"
                            " entry_price, shares, size_usdc, z_at_fill, opened_at)"
                            " VALUES (?,?,?,?,?,?,?,?)",
                            (o["window_start"], o["direction"], o["token_id"],
                             BID_PRICE, matched, size, o["z"], time.time()))
                        await db.commit()
                        trade_id = cur.lastrowid
                    except aiosqlite.IntegrityError:
                        trade_id = None
                self.position = dict(id=trade_id, **o, matched=matched, size=size,
                                     strike=self.btc_open)
                self.order = None
                if matched < o["shares"]:
                    try:
                        await pm.cancel_order(PRIVATE_KEY, self.api_key,
                                              self.api_secret, self.api_passphrase,
                                              o["order_id"])
                    except Exception:
                        pass
                bal = await self.balance()
                await notify(f"🟢 copy99 FILL {o['direction']} {matched:.1f} sh @ 0.99 "
                             f"(${size:.2f}) | z={o['z']:.2f}\n"
                             f"💰 Capital : ${bal + size:.2f} (${bal:.2f} cash + ${size:.2f} en jeu)")
                logger.info("FILL %s %.1f sh", o["direction"], matched)
            elif status in ("CANCELED", "EXPIRED", "DEAD"):
                self.order = None

    async def _poly_verdict(self, token_id):
        """True/False (gagné selon le book Polymarket) ou None si indispo."""
        for side in ("SELL", "BUY"):
            try:
                p = await pm.get_best_price(token_id, side)
            except Exception:
                p = None
            if p is not None:
                return p > 0.5
        return None

    async def _oracle_verdict(self, pos):
        """Verdict indépendant depuis le close Binance vs strike de la fenêtre.
        None si close ou strike indisponible."""
        strike = pos.get("strike")
        if not strike:
            return None
        close = await pm.get_btc_5m_close(pos["window_start"])
        if close is None:
            return None
        up = close > strike
        return up if pos["direction"] == "UP" else (not up)

    async def resolution_loop(self):
        while True:
            await asyncio.sleep(3)
            pos = self.position
            if not pos:
                continue
            end = pos["window_start"] + WINDOW_SECS
            if time.time() < end + 6:
                continue
            # deux sources indépendantes : book Polymarket (règlement effectif)
            # + close Binance vs strike (oracle prix, quasi-identique à Chainlink)
            poly = await self._poly_verdict(pos["token_id"])
            oracle = await self._oracle_verdict(pos)
            votes = [v for v in (poly, oracle) if v is not None]
            if not votes:
                continue                       # aucune donnée -> on repolle
            if all(votes):
                won = True                     # consensus : gagné
            elif not any(votes):
                won = False                    # consensus : perdu
            else:
                # désaccord book/oracle (BTC ~ au strike). Le BOOK Polymarket est le
                # règlement RÉEL -> on attend qu'il se fige puis on tranche dessus.
                # Notif UNE SEULE fois par position (sinon spam toutes les 3s).
                if not pos.get("_notified"):
                    await notify(f"⚠️ {STRATEGY.upper()} {pos['direction']} : book/oracle "
                                 f"divergent près du strike (poly={poly} oracle={oracle}) — "
                                 f"attente du règlement…")
                    logger.warning("resolution désaccord poly=%s oracle=%s", poly, oracle)
                    pos["_notified"] = True
                if poly is not None and time.time() > end + 90:
                    won = bool(poly)           # timeout -> le book (règlement réel) fait foi
                else:
                    continue                   # re-polle EN SILENCE
            # annuler l'ordre de vente 0.99 restant + comptabiliser le partiel déjà vendu
            sold = pos.get("_sold", 0.0)
            if pos.get("sell_order"):
                try:
                    st = await pm.get_order_status(PRIVATE_KEY, self.api_key, self.api_secret,
                                                   self.api_passphrase, pos["sell_order"])
                    if isinstance(st, dict):
                        sold = max(sold, float(st.get("size_matched") or 0))
                except Exception:
                    pass
                try:
                    await pm.cancel_order(PRIVATE_KEY, self.api_key, self.api_secret,
                                          self.api_passphrase, pos["sell_order"])
                except Exception:
                    pass
            remaining = max(pos["matched"] - sold, 0.0)
            proceeds = sold * REV_TP + remaining * (1.0 if won else 0.0)
            pnl = round(proceeds - pos["size"], 2)
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("UPDATE trades SET resolved=1, won=?, pnl_usdc=? WHERE id=?",
                                 (int(won), pnl, pos["id"]))
                await db.commit()
            self.position = None
            bal = await self.balance()
            filt = " AND entry_price < 0.9" if STRATEGY == "reversal" else ""
            async with aiosqlite.connect(DB_PATH) as db:
                cur = await db.execute("SELECT COUNT(*), COALESCE(SUM(pnl_usdc),0),"
                                       " COALESCE(SUM(won),0) FROM trades WHERE resolved=1" + filt)
                n, tot, w = await cur.fetchone()
            if STRATEGY == "reversal":
                await notify(f"{'✅' if won else '❌'} REVERSAL {pos['direction']} "
                             f"{'GAGNÉ' if won else 'PERDU'} {pnl:+.2f}$")
            else:
                await notify(f"{'✅' if won else '❌'} copy99 {'gagné' if won else 'PERDU'} "
                             f"{pnl:+.2f}$ | total {tot:+.2f}$ ({w}/{n})\n"
                             f"💰 Capital total : ${bal:.2f}")
            if not won and STOP_ON_LOSS and STRATEGY == "maker":
                self.stopped = True
                await notify("🛑 copy99 : PREMIÈRE PERTE — arrêt total (breakeven 99% : "
                             "une perte est un signal statistique fort). On analyse "
                             "avant de continuer.")
                logger.warning("STOP ON LOSS")

    async def protect_loop(self):
        """Stop-loss protecteur : vend si BTC repasse du mauvais côté du strike
        (notre token plonge vers ~0.50), pour écrêter la perte."""
        while True:
            await asyncio.sleep(1)
            if not PROTECT_EXIT or STRATEGY != "maker":
                continue
            pos = self.position
            if not pos or self._exiting or self.stopped:
                self._uw = 0
                continue
            if self.btc_price is None or not pos.get("strike"):
                continue
            remain = pos["window_start"] + WINDOW_SECS - time.time()
            if remain < EXIT_MIN_REMAIN:
                continue                          # trop tard : on laisse résoudre
            margin = (self.btc_price - pos["strike"]) if pos["direction"] == "UP" \
                     else (pos["strike"] - self.btc_price)
            if margin >= 0:
                self._uw = 0                      # du bon côté : rien à faire
                continue
            self._uw += 1
            if self._uw < EXIT_CONFIRM or time.time() < self._exit_cd:
                continue
            self._exiting = True
            self._exit_cd = time.time() + EXIT_COOLDOWN
            try:
                await self._protective_exit(pos)
            except Exception as e:
                logger.error("protective_exit: %s", e)
            finally:
                self._exiting = False
                self._uw = 0

    async def _protective_exit(self, pos):
        logger.warning("STOP-LOSS %s sous le strike (btc=%.2f strike=%.2f) → vente",
                       pos["direction"], self.btc_price, pos["strike"])
        fill = None
        for L in EXIT_FLOORS:
            try:
                res = await pm.sell_shares(PRIVATE_KEY, self.api_key, self.api_secret,
                                           self.api_passphrase, pos["token_id"],
                                           pos["matched"], round(L, 4))
                status = (res.get("status") or "").upper() if isinstance(res, dict) else ""
                if status in ("MATCHED", "FILLED"):
                    fill = L
                    break
                logger.warning("exit FOK non rempli @%.2f: %s", L, res)
            except Exception as e:
                if "couldn't be fully filled" in str(e) or "FOK" in str(e):
                    logger.info("exit FOK tué @%.2f", L)
                else:
                    logger.error("exit sell @%.2f: %s", L, e)
            await asyncio.sleep(0.3)
        if fill is None:
            # vente impossible (carnet vide) : on laisse la résolution constater la
            # perte pleine → STOP_ON_LOSS arrêtera le bot.
            await notify("⚠️ copy99 STOP-LOSS déclenché mais vente impossible "
                         "(carnet vide) — position tenue jusqu'à expiry.")
            return
        pnl = round(pos["matched"] * fill - pos["size"], 2)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE trades SET resolved=1, won=0, pnl_usdc=? WHERE id=?",
                             (pnl, pos["id"]))
            await db.commit()
        self.position = None
        bal = await self.balance()
        await notify(f"🛡️ copy99 STOP-LOSS {pos['direction']} — vendu ~{fill:.2f} | "
                     f"perte écrêtée {pnl:+.2f}$ (au lieu de ~-${pos['size']:.2f}) — on continue.\n"
                     f"💰 Solde : ${bal:.2f}")
        logger.warning("STOP-LOSS rempli @%.2f pnl=%+.2f → on continue", fill, pnl)

    async def stats_loop(self):
        """Log périodique (fichier). ⚠️ NE PLUS logger SUM(pnl_usdc) : cette somme ne
        couvre QUE les trades du bot et diverge du solde réel dès qu'il y a des trades
        manuels (16/07 : base "+84.67$" vs solde réel $4.07). On logge l'ARGENT RÉEL."""
        while True:
            await asyncio.sleep(600)
            filt = " AND entry_price < 0.9" if STRATEGY == "reversal" else ""
            async with aiosqlite.connect(DB_PATH) as db:
                cur = await db.execute("SELECT COUNT(*), COALESCE(SUM(won),0)"
                                       " FROM trades WHERE resolved=1" + filt)
                n, w = await cur.fetchone()
            try:
                bal = await self.balance()
            except Exception:
                bal = self._dash_bal
            base = self.rev_baseline or 0.0
            logger.info("[%s] %d résolus | %d gagnés (%.1f%%) | solde RÉEL $%.2f "
                        "| PnL réel %+.2f$ | stopped=%s", STRATEGY, n, w,
                        (w / n * 100) if n else 0.0, bal, bal - base, self.stopped)


async def main():
    global _BOT
    acquire_lock("copy99_bot")
    await init_db()
    bot = Copy99()
    _BOT = bot                           # expose l'instance au dashboard terminal
    await bot.load_creds()
    await bot.reload_position()          # ne pas abandonner une position ouverte au restart
    # garde-fou anti-crash : annule tout bid resté au repos d'une session tuée
    # (sinon il pourrait se remplir dans une fenêtre qu'on ne surveille plus)
    try:
        orphans = await pm.get_open_orders(PRIVATE_KEY, bot.api_key,
                                           bot.api_secret, bot.api_passphrase)
        for o in orphans or []:
            oid = o.get("id") or o.get("orderID")
            try:
                await pm.cancel_order(PRIVATE_KEY, bot.api_key, bot.api_secret,
                                      bot.api_passphrase, oid)
            except Exception:
                pass
        if orphans:
            logger.warning("startup: %d ordre(s) orphelin(s) annulé(s)", len(orphans))
    except Exception as e:
        logger.warning("startup cleanup: %s", e)
    bal = await bot.balance()
    bot.rev_baseline = REV_BASELINE      # référence PnL = solde de départ reversal
    if STRATEGY == "reversal":
        # reflète les RÉGLAGES RÉELS (menu ⚙️ / reversal_settings.json), pas des constantes
        cfg = bot.settings
        st = bot._rev_stake(bal)
        mise = (f"${cfg['bet_value']:.2f} fixe" if cfg["bet_mode"] == "fixed"
                else f"{cfg['bet_value']:.1f}% du solde (= ${st:.0f})")
        logger.info("REVERSAL démarré | solde $%.2f | mise %s | <=%.2f puis >=%.2f (max %.2f) | "
                    "fenêtre %d-%ds | no stop-loss | UP+DOWN",
                    bal, mise, cfg["low"], cfg["trigger"], REV_MAX_ENTRY, cfg["tmin"], cfg["tmax"])
        await notify(
            f"🔄 REVERSAL démarré (LIVE) — momentum breakout UP+DOWN\n"
            f"Achat taker quand un token vu ≤{cfg['low']} repasse ≥{cfg['trigger']} "
            f"(max {REV_MAX_ENTRY}) | mise {mise}\n"
            f"⚠️ Hold to expiry, AUCUN stop-loss (choix Tom). Edge mesuré : "
            f"+6.5pp sur 375 trades.")
    else:
        logger.info("copy99 démarré | solde $%.2f | stake %d%% | z>=%.1f | %d-%ds restants | stop-on-loss",
                    bal, int(STAKE_PCT * 100), Z_MIN, MIN_REMAIN, MAX_REMAIN)
        await notify(
            f"🧪 copy99 démarré — réplique du wallet +$40k\n"
            f"Bid maker 0.99 quand z≥{Z_MIN} à <{MAX_REMAIN}s de la fin | "
            f"mise {int(STAKE_PCT*100)}% (${bal*STAKE_PCT:.2f}) | 1/fenêtre\n"
            f"🛡️ Stop-loss sous le strike (perte écrêtée) — arrêt total seulement si "
            f"la vente échoue.")
    global _UI
    _UI = TelegramUI(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID,
                     build_dashboard=bot.build_dashboard,
                     on_action=bot.on_ui_action,
                     viewer_usernames=VIEWER_USERNAMES,
                     apply_setting=bot.apply_setting,
                     get_settings=lambda: bot.settings)
    # Boucles REVERSAL uniquement. vol_loop/fill_loop/protect_loop ne servent qu'au mode
    # maker (evaluate() est gaté sur STRATEGY=="maker" dans binance_loop) -> non lancées.
    await asyncio.gather(
        bot.window_loop(),        # suit la fenêtre 5m + les tokens UP/DOWN
        bot.binance_loop(),       # prix BTC live (WS)
        bot.rev_loop(),           # entrée reversal + gestion de la vente 0.99
        bot.resolution_loop(),    # résolution + PnL
        bot.stats_loop(),         # log périodique
        _UI.run(lambda: bot.paused or bot.stopped),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Arrêt demandé.")
