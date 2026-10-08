"""Configuration chargée depuis .env — aucun fallback sur des valeurs sensibles."""
import os
from dotenv import load_dotenv

load_dotenv()

# ── Telegram ──────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN: str = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID: int   = int(os.environ["TELEGRAM_CHAT_ID"])

# ── Wallet ────────────────────────────────────────────────────────────────────
PRIVATE_KEY: str = os.environ["PRIVATE_KEY"]

# ── Encryption key (obligatoire, aucun fallback) ──────────────────────────────
_enc = os.getenv("ENCRYPTION_KEY", "")
if not _enc:
    raise RuntimeError(
        "ENCRYPTION_KEY manquant.\n"
        "Génère-le : python3 -c \"import secrets; print(secrets.token_hex(32))\"\n"
        "Puis ajoute ENCRYPTION_KEY=<valeur> dans ton .env"
    )
ENCRYPTION_KEY: str = _enc

# ── Polygon RPC ───────────────────────────────────────────────────────────────
ALCHEMY_API_KEY: str = os.getenv("ALCHEMY_API_KEY", "")
POLYGON_RPC_URL: str = (
    f"https://polygon-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"
    if ALCHEMY_API_KEY
    else "https://polygon-rpc.com"
)
CHAIN_ID = 137

# ── Contrats Polymarket (Polygon mainnet) ─────────────────────────────────────
CTF_EXCHANGE = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
CTF_CONTRACT = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
USDC_ADDRESS = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"

# ── Polymarket APIs ───────────────────────────────────────────────────────────
CLOB_API  = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"

# ── Proxy ─────────────────────────────────────────────────────────────────────
PROXY_URL:   str = os.getenv("PROXY_URL", "")
WEBAPP_URL:  str = os.getenv("WEBAPP_URL", "")

# ── Storage ───────────────────────────────────────────────────────────────────
DB_PATH = "bot.db"

# ── Paramètres de la stratégie ────────────────────────────────────────────────
BET_SIZE_PCT             = 0.05   # taille de mise = 5% de la balance courante
BET_SIZE_MIN_USDC        = 1.0    # plancher : jamais moins de $1 par bet
BET_SIZE_MAX_USDC        = 100.0  # plafond  : jamais plus de $100 par bet
ENTRY_MAX_PRICE          = 0.45   # n'achète que si prix ≤ cette valeur
ENTRY_MIN_PRICE          = 0.08   # n'achète PAS si le prix est déjà écrasé sous ce seuil
                                   # (un prix < 8c signifie que le marché considère l'issue
                                   #  quasi certaine — ce n'est plus une "surréaction" fadable,
                                   #  c'est de l'info. Acheter là = "free loss" systématique.)
KILL_BALANCE             = 1   # pause si balance < ce seuil ($)
MAX_WINDOW_AGE           = 240    # pas d'entrée après 4m00 dans la bougie (marge de sécurité
                                   # sur NO_ENTRY_SECONDS_BEFORE_CLOSE, voir plus bas)
MIN_WINDOW_AGE           = 3      # pas d'entrée dans les 3 premières secondes (bruit d'ouverture)
NO_ENTRY_SECONDS_BEFORE_CLOSE = 60  # AUCUNE entrée dans les 60 dernières secondes de la fenêtre.
                                     # C'est le fix principal du bug "achète à 1c avec 1-2s
                                     # restantes puis compte un free loss dans la fenêtre suivante" :
                                     # en toute fin de fenêtre, un crash de prix reflète une
                                     # résolution quasi actée, pas du bruit de carnet.
MIN_SECONDS_BETWEEN_BETS = 0    # cooldown entre deux bets
CIRCUIT_BREAKER_LOSSES   = 999      # nb de pertes consécutives avant circuit breaker
CIRCUIT_BREAKER_WINDOWS  = 999      # nb de fenêtres en pause après circuit breaker

# ── Signal spike ──────────────────────────────────────────────────────────────
SPIKE_LOOKBACK    = 2      # fenêtre de lookback pour détecter un spike (secondes)
SPIKE_MIN_DROP    = 0.12   # chute minimale du prix ask pour déclencher l'entrée
TRAIL_PROFIT_MIN_PCT = 0.50  # le trailing s'active quand bid >= entry * (1 + 0.50)
                              # ex : entrée 18c → trail actif à 27c, entrée 40c → 60c
TRAIL_DROP           = 0.05  # recul depuis le pic bid pour déclencher la vente

# ── Stop-loss & sortie forcée (2026-07-07 — analyse des 83 premiers trades) ───
# Donnée clé : sur 83 trades, les 43 tenus jusqu'à expiry (jamais sortis par
# take-profit) ont fait 0/43 win rate (0%). Les 40 sortis par take-profit ont
# fait 82% de win rate. Conclusion : le "mean reversion" ne se vérifie QUE sur
# un rebond de prix à court terme (qui déclenche le take-profit) — si ce rebond
# n'arrive pas vite, la position ne "se corrige" quasiment jamais avant
# l'expiry, elle perd 100% du stake. Tenir jusqu'au bout est donc -EV presque
# par construction. Deux garde-fous ajoutés en conséquence :
STOP_LOSS_PCT = 0.35    # coupe si le bid tombe à -35% de l'entrée (relatif, pas en cents)
                         # ex : entrée 0.40 → stop si bid <= 0.26 ; entrée 0.18 → stop si bid <= 0.117
                         # tout de suite plutôt que d'attendre une expiry quasi-certaine.
FORCE_EXIT_SECONDS_BEFORE_CLOSE = 20  # dans les 20 dernières secondes, si toujours en
                                       # position (ni take-profit ni stop-loss déclenché),
                                       # on vend au bid courant quel qu'il soit : n'importe
                                       # quel prix de sortie > 0 bat une expiry à 0% de WR.

# ── Seuil de sortie pre-expiry fee-aware (remplace le seuil naïf +0.005$) ─────
# L'ancien seuil ("bid > entry + 0.005") est plus petit que le coût d'un
# aller-retour (achat + vente), donc plusieurs sorties "take_profit" étaient
# en réalité des pertes nettes après fees (7 sur 40 dans l'historique observé).
# On exige maintenant un PnL net (post-fees, via _pnl_tp) positif d'au moins
# ce montant avant de sortir en pre-expiry "profit".
MIN_NET_PROFIT_PRE_EXPIRY = 0.03  # $ net minimum pour valider une sortie pre-expiry "profit"

# ── Confirmation directionnelle (anti adverse-selection) ──────────────────────
# On ne "fade" (rachète) un spike que si le BTC lui-même n'a pas déjà confirmé
# le mouvement dans le sens qui ferait perdre le pari. Sinon le spike n'est pas
# une surréaction : c'est le marché qui a raison.
BTC_CONFIRM_MAX_ADVERSE = 60.0  # $ de mouvement BTC vs open 5m au-delà duquel on ne fade plus

# ── Résolution d'expiry (fix du bug de "free loss" au changement de fenêtre) ──
EXPIRY_RESOLUTION_TIMEOUT      = 20   # secondes après expiry pendant lesquelles on réessaie
                                      # de récupérer le prix définitif via REST avant fallback
EXPIRY_RESOLUTION_MAX_ATTEMPTS = 60   # garde-fou (avec le tick 0.2s ⇒ ~12s min)

# ── Adresse du wallet (calculée au démarrage) ─────────────────────────────────
from web3 import Web3 as _Web3
WALLET_ADDRESS: str = _Web3().eth.account.from_key(PRIVATE_KEY).address
