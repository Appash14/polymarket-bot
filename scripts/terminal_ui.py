# -*- coding: utf-8 -*-
"""Dashboard TERMINAL live — réécrit en place, aucun scroll, aucun log.

Les logs ne s'affichent PLUS dans le terminal : ils partent uniquement dans
`data/copy99.log` (cf. logging.basicConfig de copy99_bot.py). Le terminal ne
contient que ce panneau, rafraîchi en place.

⚠️ RÈGLE D'OR DU PnL : on n'affiche JAMAIS la somme des pnl_usdc de la base.
Cette somme ne décrit QUE les trades du bot — elle ignore les trades manuels et
diverge donc du solde réel (constaté le 16/07 : base "+84.67$" vs solde réel
$4.07, l'écart venant de ~$4.9k de trading manuel hors bot).
Seule vérité affichée : **l'argent réel** (solde CLOB + valeur de la position),
et le PnL = argent réel − baseline.
"""

import asyncio
import sqlite3
import sys
import time
from pathlib import Path

from rich import box
from rich.align import Align
from rich.console import Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

import polymarket as pm
from copy99_bot import REV_MAX_ENTRY

REFRESH      = 0.5      # rafraîchissement de l'affichage (s)
BAL_EVERY    = 30       # re-lecture du solde réel (s)
POSBID_EVERY = 5        # re-lecture du bid de la position ouverte (s)
STATS_EVERY  = 10       # re-lecture des stats DB (s)
WINDOW_SECS  = 300

C_DIM   = "grey50"
C_LABEL = "grey62"
C_OK    = "bright_green"
C_BAD   = "bright_red"
C_ACC   = "bright_cyan"
C_WARN  = "yellow"


def _fmt_usd(v):
    return f"${v:,.2f}".replace(",", "'")


class TerminalUI:
    def __init__(self, bot, market_db: Path):
        self.bot = bot
        self.market_db = market_db
        self.db_path = None            # renseigné au run (trades)
        self._bal_ts = 0.0
        self._posbid = None
        self._posbid_ts = 0.0
        self._stats_ts = 0.0
        self._stats = (0, 0, 0.0)      # n, wins, avg_fill
        self._collect = (0, 0, 0, 0)

    # ── données (lectures espacées : le terminal ne doit rien coûter) ─────────

    async def _refresh_data(self):
        b, now = self.bot, time.time()
        if now - self._bal_ts > BAL_EVERY:
            try:
                b._dash_bal = await b.balance()
                b._dash_bal_ts = now
                self._bal_ts = now
            except Exception:
                pass
        if b.position and now - self._posbid_ts > POSBID_EVERY:
            try:
                self._posbid = await pm.get_best_price(b.position["token_id"], "BUY")
            except Exception:
                pass
            self._posbid_ts = now
        if not b.position:
            self._posbid = None
        if now - self._stats_ts > STATS_EVERY:
            self._stats_ts = now
            try:
                d = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
                self._stats = d.execute(
                    "SELECT COUNT(*), COALESCE(SUM(won),0),"
                    " COALESCE(AVG(CASE WHEN shares>0 THEN size_usdc/shares END),0)"
                    " FROM trades WHERE resolved=1 AND entry_price<0.9").fetchone()
                d.close()
            except Exception:
                pass
            try:
                d = sqlite3.connect(f"file:{self.market_db}?mode=ro", uri=True)
                self._collect = tuple(
                    d.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                    for t in ("poly_top", "poly_trades", "poly_book", "klines"))
                d.close()
            except Exception:
                pass

    # ── rendu ────────────────────────────────────────────────────────────────

    def _row(self, grid, label, value):
        grid.add_row(Text(f"  {label}", style=C_LABEL), value)

    def _render(self):
        b = self.bot
        now = time.time()
        cfg = b.settings

        # ── état
        if b.stopped:
            state = Text("● STOPPÉ", style=f"bold {C_BAD}")
        elif b.paused:
            state = Text("● PAUSE", style=f"bold {C_WARN}")
        else:
            state = Text("● LIVE", style=f"bold {C_OK}")

        # ── ARGENT RÉEL (jamais la somme des pnl : cf. docstring)
        pos_val = 0.0
        if b.position:
            pos_val = (b.position["matched"] * self._posbid) if self._posbid is not None \
                      else b.position["size"]
        money = (b._dash_bal or 0.0) + pos_val
        base = b.rev_baseline or 0.0
        pnl = money - base if base else 0.0
        pct = (pnl / base * 100) if base else 0.0
        pcol = C_OK if pnl >= 0 else C_BAD

        n, w, avg_fill = self._stats
        wr = (w / n * 100) if n else 0.0
        edge = wr - avg_fill * 100 if n else 0.0

        g = Table.grid(padding=(0, 2), expand=True)
        g.add_column(width=14, no_wrap=True)
        g.add_column(ratio=1)

        money_t = Text()
        money_t.append(_fmt_usd(money), style="bold white")
        money_t.append("      PnL ", style=C_LABEL)
        money_t.append(f"{pnl:+.2f}$", style=f"bold {pcol}")
        money_t.append(f"  ({pct:+.1f}%)", style=pcol)
        money_t.append(f"   base {_fmt_usd(base)}", style=C_DIM)
        self._row(g, "SOLDE RÉEL", money_t)

        wr_t = Text()
        wr_t.append(f"{wr:.2f}%", style="bold white")
        wr_t.append(f"   {w}W {n-w}L ", style=C_LABEL)
        wr_t.append(f"({n} trades)", style=C_DIM)
        self._row(g, "WIN RATE", wr_t)

        f_t = Text()
        f_t.append(f"~{avg_fill:.3f}", style="bold white")
        f_t.append(f"   breakeven {avg_fill*100:.2f}%   edge ", style=C_LABEL)
        f_t.append(f"{edge:+.2f}pp", style=C_OK if edge > 0 else C_BAD)
        self._row(g, "FILL MOYEN", f_t)

        g.add_row("", "")

        # ── BTC
        btc_t = Text()
        if b.btc_price and b.btc_open:
            diff = b.btc_price - b.btc_open
            dtxt = f"{diff:+.2f}" if abs(diff) < 1 else f"{diff:+.0f}"
            btc_t.append(f"{b.btc_open:,.0f}".replace(",", "'"), style=C_DIM)
            btc_t.append("  →  ", style=C_DIM)
            btc_t.append(f"{b.btc_price:,.0f}".replace(",", "'"), style="bold white")
            btc_t.append(f"   {dtxt}$ ", style=C_OK if diff >= 0 else C_BAD)
            btc_t.append("▲" if diff >= 0 else "▼", style=C_OK if diff >= 0 else C_BAD)
        else:
            btc_t.append("en attente…", style=C_DIM)
        self._row(g, "₿ BITCOIN", btc_t)

        # ── marché : asks lus depuis le cache du bot (0 appel API)
        up_a = (b._rev.get(b.up_token) or {}).get("ask")
        dn_a = (b._rev.get(b.down_token) or {}).get("ask")
        remain = max(0, int(b.window_start + WINDOW_SECS - now)) if b.window_start else 0
        mk = Text()
        mk.append("UP ", style=C_LABEL)
        mk.append(f"{up_a:.2f}" if up_a else "–", style="bold white")
        mk.append("   │   ", style=C_DIM)
        mk.append("DOWN ", style=C_LABEL)
        mk.append(f"{dn_a:.2f}" if dn_a else "–", style="bold white")
        mk.append(f"        ⏱ {remain//60}m{remain%60:02d}s", style=C_ACC)
        self._row(g, "MARCHÉ", mk)

        g.add_row("", "")

        # ── position
        p_t = Text()
        if b.position:
            p = b.position
            cost = p["size"]
            ppct = ((pos_val - cost) / cost * 100) if cost else 0.0
            p_t.append(f"{p['matched']:.2f} {p['direction']} ", style=f"bold {C_ACC}")
            p_t.append(f"@ {p.get('entry', 0):.2f}  —  {_fmt_usd(cost)}", style="white")
            p_t.append(f"   valeur {_fmt_usd(pos_val)} ", style=C_LABEL)
            p_t.append(f"({ppct:+.0f}%)", style=C_OK if ppct >= 0 else C_BAD)
        else:
            p_t.append("— en recherche d'un retournement", style=C_DIM)
        self._row(g, "POSITION", p_t)

        # ── réglages
        mise = (f"${cfg['bet_value']:.0f}" if cfg["bet_mode"] == "fixed"
                else f"{cfg['bet_value']:.0f}%")
        tp = "0.99" if cfg.get("tp_mode") == "sell99" else "résol."
        s_t = Text(f"mise {mise} · ≤{cfg['low']}→≥{cfg['trigger']} · max {REV_MAX_ENTRY} · "
                   f"TP {tp} · {cfg['tmin']}-{cfg['tmax']}s", style=C_DIM, no_wrap=True)
        self._row(g, "RÉGLAGES", s_t)

        c = self._collect
        c_t = Text(f"{c[0]:,} top  ·  {c[1]:,} trades  ·  {c[2]:,} books  ·  {c[3]:,} klines"
                   .replace(",", " "), style=C_DIM)
        self._row(g, "COLLECTE", c_t)

        # ── entête
        head = Table.grid(expand=True)
        head.add_column(ratio=1)
        head.add_column(justify="right")
        head.add_row(Text("◆  APPASH POLYMARKET BOT", style=f"bold {C_ACC}"), state)

        foot = Align.center(
            Text(f"logs → data/copy99.log   ·   Ctrl-C pour arrêter   ·   "
                 f"{time.strftime('%H:%M:%S')}", style=C_DIM))

        return Panel(Group(head, Text(""), g, Text(""), foot),
                     box=box.ROUNDED,          # cadre arrondi (le défaut peut tomber en carré)
                     border_style=C_ACC if not b.stopped else C_BAD,
                     padding=(1, 2))

    # ── boucle ───────────────────────────────────────────────────────────────

    async def run(self, db_path):
        self.db_path = db_path
        with Live(self._render(), refresh_per_second=4, screen=False) as live:
            while True:
                try:
                    await self._refresh_data()
                    live.update(self._render())
                except Exception:
                    pass                       # le terminal ne doit JAMAIS tuer le bot
                await asyncio.sleep(REFRESH)
