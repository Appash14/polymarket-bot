# -*- coding: utf-8 -*-
"""Point d'entrée UNIQUE du projet.

    python bot.py

Lance exactement 3 choses, en parallèle :

  1. le BOT REVERSAL          (trading live + dashboard Telegram)
  2. le COLLECTEUR POLYMARKET (prix UP/DOWN de chaque fenêtre BTC 5m, toutes les 2s)
  3. le COLLECTEUR DE BOUGIES (klines BTC/USDT 5m closes, Binance)

Rien d'autre. (Le monitoring du whale et la stratégie maker copy99 sont abandonnés ;
l'ancien code est dans archive/2026-07-16_cleanup/.)

Bases de données :
  data/copy99.db       -> trades (historique conservé, nom legacy)
  data/market_data.db  -> données collectées (poly_ticks + klines)
"""

import asyncio
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import collectors
import copy99_bot as reversal   # noqa: E402  (module legacy : le bot reversal)
from single_instance import running_pid
from terminal_ui import TerminalUI

logger = logging.getLogger("bot")


async def _terminal():
    """Dashboard terminal — démarre dès que le bot a fini son init."""
    while reversal._BOT is None:
        await asyncio.sleep(0.2)
    ui = TerminalUI(reversal._BOT, market_db=collectors.DATA_DB)
    await ui.run(reversal.DB_PATH)


async def main():
    await collectors.init_data_db()
    logger.info("=== DÉMARRAGE : reversal + collecteur Polymarket + collecteur klines ===")
    # Si une tâche meurt, on veut le savoir (et pas un silence) -> return_exceptions=False
    await asyncio.gather(
        reversal.main(),                    # bot reversal (acquiert son lock, gère l'UI)
        collectors.poly_ws_collector(),     # WS CLOB : top-of-book + trades + profondeur
        collectors._flusher(),              # écriture bufferisée en base (INDISPENSABLE)
        collectors.kline_collector(),       # bougies 5m closes
        collectors.stats_loop(),            # log de collecte toutes les 5 min
        _terminal(),                        # dashboard live (le terminal n'affiche que ça)
    )


if __name__ == "__main__":
    # Contrôle AVANT de rien lancer : sinon les collecteurs démarrent, écrivent une
    # fraction de seconde en base, puis meurent salement quand le bot refuse le lock.
    _pid = running_pid("copy99_bot")
    if _pid:
        print(f"⛔ Le bot tourne DÉJÀ (PID {_pid}) — rien n'a été lancé.\n"
              f"   Pour le relancer :  taskkill /PID {_pid} /F   puis   python bot.py\n"
              f"   (deux instances = trades en double + conflit Telegram 409)")
        sys.exit(1)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Arrêt demandé.")
