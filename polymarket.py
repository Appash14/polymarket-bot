"""
Client Polymarket CLOB.
Toutes les fonctions bloquantes sont wrappées dans asyncio.to_thread().
Le proxy (PROXY_URL dans .env) est appliqué à toutes les requêtes Polymarket.
"""
import asyncio
import logging
import os
from typing import Optional

import aiohttp
from web3 import Web3
from py_clob_client_v2 import (
    ClobClient, ApiCreds, MarketOrderArgsV2, OrderArgsV2, OrderType, Side,
    SignatureTypeV2, OpenOrderParams,
)

from config import (
    CLOB_API, GAMMA_API, CHAIN_ID, POLYGON_RPC_URL,
    CTF_EXCHANGE, CTF_CONTRACT, USDC_ADDRESS, PROXY_URL,
)

# Deposit wallet for V2 POLY_1271 orders (pre-deployed, holds pUSD), lu dans .env
DEPOSIT_WALLET = os.environ.get("DEPOSIT_WALLET", "")

logger = logging.getLogger(__name__)

_MAX_UINT256 = 2**256 - 1

_ERC20_APPROVE_ABI = [{
    "inputs": [{"name": "spender", "type": "address"}, {"name": "amount", "type": "uint256"}],
    "name": "approve",
    "outputs": [{"name": "", "type": "bool"}],
    "stateMutability": "nonpayable",
    "type": "function",
}]

_CTF_APPROVE_ABI = [{
    "inputs": [{"name": "operator", "type": "address"}, {"name": "approved", "type": "bool"}],
    "name": "setApprovalForAll",
    "outputs": [],
    "stateMutability": "nonpayable",
    "type": "function",
}]


# ── Proxy helpers ─────────────────────────────────────────────────────────────

def _apply_proxy_env() -> None:
    """Injecte le proxy dans les variables d'env pour que requests (py_clob_client) l'utilise."""
    if PROXY_URL:
        os.environ["HTTP_PROXY"]  = PROXY_URL
        os.environ["HTTPS_PROXY"] = PROXY_URL
        os.environ["http_proxy"]  = PROXY_URL
        os.environ["https_proxy"] = PROXY_URL


def _aiohttp_session() -> aiohttp.ClientSession:
    """Crée une session aiohttp avec proxy si configuré."""
    connector = None
    if PROXY_URL and PROXY_URL.startswith("socks"):
        try:
            from aiohttp_socks import ProxyConnector
            connector = ProxyConnector.from_url(PROXY_URL)
        except ImportError:
            logger.warning("aiohttp_socks non installé — proxy SOCKS ignoré pour les appels REST. Installe-le : pip install aiohttp-socks")
    return aiohttp.ClientSession(connector=connector)


# ── Client factory ────────────────────────────────────────────────────────────

def _make_client(private_key: str, creds: Optional[ApiCreds] = None) -> ClobClient:
    _apply_proxy_env()
    return ClobClient(
        host=CLOB_API,
        chain_id=CHAIN_ID,
        key=private_key,
        creds=creds,
        signature_type=SignatureTypeV2.POLY_1271,
        funder=DEPOSIT_WALLET,
    )


# ── Approbations on-chain ─────────────────────────────────────────────────────

def _make_w3() -> Web3:
    from web3.middleware import ExtraDataToPOAMiddleware
    # Le RPC Polygon (Alchemy) ne nécessite pas de proxy car c'est une API dédiée
    w3 = Web3(Web3.HTTPProvider(POLYGON_RPC_URL))
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    return w3


# Chainlink BTC/USD aggregator on Polygon mainnet (same oracle Polymarket uses)
_CHAINLINK_BTC_USD = Web3.to_checksum_address("0xc907E116054Ad103354f2D350FD2514433D57F6d")
_CHAINLINK_ABI = [
    {
        "inputs": [],
        "name": "latestAnswer",
        "outputs": [{"name": "", "type": "int256"}],
        "stateMutability": "view",
        "type": "function",
    }
]

def _get_chainlink_btc_sync() -> float:
    w3       = _make_w3()
    contract = w3.eth.contract(address=_CHAINLINK_BTC_USD, abi=_CHAINLINK_ABI)
    answer   = contract.functions.latestAnswer().call()
    return answer / 1e8  # Chainlink uses 8 decimal places

async def get_binance_5m_candles(limit: int = 13) -> Optional[list]:
    """Retourne les N dernières bougies 5m Binance (OHLCV).
    Chaque entry: [open_time, open, high, low, close, volume, ...]"""
    try:
        url = (f"https://api.binance.com/api/v3/klines"
               f"?symbol=BTCUSDT&interval=5m&limit={limit}")
        async with aiohttp.ClientSession() as sess:
            async with sess.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
                return await r.json()
    except Exception as e:
        logger.debug("get_binance_5m_candles error: %s", e)
        return None


def _sell_shares_sync(
    private_key: str, api_key: str, api_secret: str, api_passphrase: str,
    token_id: str, shares: float, limit_price: float,
) -> dict:
    creds  = ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase)
    client = _make_client(private_key, creds)
    order = client.create_market_order(MarketOrderArgsV2(
        token_id=token_id,
        amount=round(shares, 4),  # SELL: amount = nombre de shares
        side=Side.SELL,
        price=round(limit_price, 4),
        order_type=OrderType.FOK,
    ))
    return client.post_order(order, OrderType.FOK)


async def sell_shares(
    private_key: str, api_key: str, api_secret: str, api_passphrase: str,
    token_id: str, shares: float, limit_price: float,
) -> dict:
    """Vend `shares` tokens à `limit_price` (FOK). Pour le take profit."""
    return await asyncio.to_thread(
        _sell_shares_sync,
        private_key, api_key, api_secret, api_passphrase,
        token_id, shares, limit_price,
    )


async def get_btc_atr(interval_seconds: int = 300, periods: int = 4) -> Optional[float]:
    """Average True Range of last N Binance candles for the given interval (seconds)."""
    if interval_seconds <= 60:
        interval = "1m"
    elif interval_seconds <= 180:
        interval = "3m"
    else:
        interval = "5m"
    try:
        url = (f"https://api.binance.com/api/v3/klines"
               f"?symbol=BTCUSDT&interval={interval}&limit={periods + 1}")
        async with aiohttp.ClientSession() as sess:
            async with sess.get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
                klines = await r.json()
                if not klines or len(klines) < 2:
                    return None
                trs = []
                for i in range(1, len(klines)):
                    high       = float(klines[i][2])
                    low        = float(klines[i][3])
                    prev_close = float(klines[i - 1][4])
                    tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
                    trs.append(tr)
                return round(sum(trs) / len(trs), 2) if trs else None
    except Exception as e:
        logger.debug("get_btc_atr error: %s", e)
        return None


async def get_btc_5m_open() -> Optional[float]:
    """Return Binance 5m kline open price for the current window."""
    import time as _time
    win_start = (int(_time.time()) // 300) * 300
    try:
        url = (f"https://api.binance.com/api/v3/klines"
               f"?symbol=BTCUSDT&interval=5m&startTime={win_start*1000}&limit=1")
        async with aiohttp.ClientSession() as sess:
            async with sess.get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
                klines = await r.json()
                if klines and isinstance(klines, list):
                    return float(klines[0][1])
    except Exception as e:
        logger.debug("get_btc_5m_open error: %s", e)
    return None


async def get_btc_5m_close(window_start: int) -> Optional[float]:
    """Return Binance 5m kline CLOSE for the window starting at `window_start`
    (unix seconds). Only meaningful once that window has ended."""
    try:
        url = (f"https://api.binance.com/api/v3/klines"
               f"?symbol=BTCUSDT&interval=5m&startTime={window_start*1000}&limit=1")
        async with aiohttp.ClientSession() as sess:
            async with sess.get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
                klines = await r.json()
                if klines and isinstance(klines, list) and klines[0][0] // 1000 == window_start:
                    return float(klines[0][4])
    except Exception as e:
        logger.debug("get_btc_5m_close error: %s", e)
    return None


async def get_binance_btc_price() -> Optional[float]:
    """Return the current Binance BTC/USDT spot price (same feed as klines)."""
    try:
        url = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
        async with aiohttp.ClientSession() as sess:
            async with sess.get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
                data = await r.json()
                return float(data["price"])
    except Exception as e:
        logger.debug("get_binance_btc_price error: %s", e)
        return None


async def get_chainlink_btc_price() -> Optional[float]:
    """Return the Chainlink BTC/USD price (same feed as Polymarket oracle)."""
    try:
        return await asyncio.to_thread(_get_chainlink_btc_sync)
    except Exception as e:
        logger.debug("get_chainlink_btc_price error: %s", e)
        return None


def _approve_sync(private_key: str) -> list[str]:
    w3      = _make_w3()
    account = w3.eth.account.from_key(private_key)
    addr    = account.address
    spender = Web3.to_checksum_address(CTF_EXCHANGE)
    hashes  = []

    usdc = w3.eth.contract(address=Web3.to_checksum_address(USDC_ADDRESS), abi=_ERC20_APPROVE_ABI)
    nonce = w3.eth.get_transaction_count(addr)
    tx = usdc.functions.approve(spender, _MAX_UINT256).build_transaction(
        {"from": addr, "nonce": nonce, "chainId": CHAIN_ID}
    )
    signed = account.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
    hashes.append(tx_hash.hex())

    ctf = w3.eth.contract(address=Web3.to_checksum_address(CTF_CONTRACT), abi=_CTF_APPROVE_ABI)
    nonce += 1
    tx = ctf.functions.setApprovalForAll(spender, True).build_transaction(
        {"from": addr, "nonce": nonce, "chainId": CHAIN_ID}
    )
    signed = account.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
    hashes.append(tx_hash.hex())

    return hashes


async def approve_contracts(private_key: str) -> list[str]:
    return await asyncio.to_thread(_approve_sync, private_key)


def _check_approvals_sync(address: str) -> tuple[bool, bool]:
    """
    Vérifie si les approbations sont déjà en place.
    Retourne (usdc_approved, ctf_approved).
    """
    _ERC20_ALLOWANCE_ABI = [{
        "inputs": [{"name": "owner", "type": "address"}, {"name": "spender", "type": "address"}],
        "name": "allowance",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    }]
    _CTF_APPROVED_ABI = [{
        "inputs": [{"name": "account", "type": "address"}, {"name": "operator", "type": "address"}],
        "name": "isApprovedForAll",
        "outputs": [{"name": "", "type": "bool"}],
        "stateMutability": "view",
        "type": "function",
    }]
    w3      = _make_w3()
    spender = Web3.to_checksum_address(CTF_EXCHANGE)
    addr    = Web3.to_checksum_address(address)

    usdc    = w3.eth.contract(address=Web3.to_checksum_address(USDC_ADDRESS), abi=_ERC20_ALLOWANCE_ABI)
    ctf     = w3.eth.contract(address=Web3.to_checksum_address(CTF_CONTRACT),  abi=_CTF_APPROVED_ABI)

    allowance    = usdc.functions.allowance(addr, spender).call()
    ctf_approved = ctf.functions.isApprovedForAll(addr, spender).call()
    return allowance > 0, ctf_approved


async def check_and_approve(private_key: str, address: str) -> str:
    """
    Vérifie les approbations. Si déjà faites → skip (pas de MATIC nécessaire).
    Si non → tente l'approbation (nécessite un peu de MATIC/POL).
    Retourne un message de statut.
    """
    try:
        usdc_ok, ctf_ok = await asyncio.to_thread(_check_approvals_sync, address)
    except Exception as e:
        return f"⚠️ Impossible de vérifier les approbations: {e}"

    if usdc_ok and ctf_ok:
        return "✅ Approbations déjà en place (pas de MATIC nécessaire)"

    # Besoin d'approuver → vérifie si MATIC dispo
    try:
        w3      = _make_w3()
        balance = w3.eth.get_balance(Web3.to_checksum_address(address))
        matic   = balance / 1e18
        if matic < 0.005:
            return (
                f"⚠️ Approbation USDC requise (première utilisation).\n"
                f"Ton wallet n'a que {matic:.5f} MATIC.\n"
                f"Tu as besoin d'environ 0.01 MATIC (~$0.01).\n"
                f"Obtiens-en via : https://faucet.polygon.technology"
            )
        await approve_contracts(private_key)
        return "✅ Approbations effectuées"
    except Exception as e:
        return f"❌ Erreur approbation: {e}"


# ── Solde USDC on-chain (Polygon) ─────────────────────────────────────────────

_ERC20_BALANCE_ABI = [{
    "inputs": [{"name": "account", "type": "address"}],
    "name": "balanceOf",
    "outputs": [{"name": "", "type": "uint256"}],
    "stateMutability": "view",
    "type": "function",
}]


def _get_usdc_balance_sync(address: str) -> float:
    w3      = _make_w3()
    usdc    = w3.eth.contract(
        address=Web3.to_checksum_address(USDC_ADDRESS),
        abi=_ERC20_BALANCE_ABI,
    )
    raw = usdc.functions.balanceOf(Web3.to_checksum_address(address)).call()
    return raw / 1e6   # USDC has 6 decimals


async def get_polygon_usdc_balance(address: str) -> float:
    """Retourne le solde USDC (Polygon) d'une adresse EOA on-chain."""
    try:
        return await asyncio.to_thread(_get_usdc_balance_sync, address)
    except Exception as e:
        logger.debug("get_polygon_usdc_balance error: %s", e)
        return 0.0


def _get_clob_balance_sync(private_key: str, api_key: str, api_secret: str, api_passphrase: str) -> float:
    """Retourne le solde pUSD disponible dans le deposit wallet Polymarket V2."""
    try:
        from web3 import Web3 as _Web3
        import json as _json
        pUSD = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
        _ERC20_BALANCE_ABI = _json.loads('[{"inputs":[{"name":"account","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"type":"function"}]')
        w3 = _Web3(_Web3.HTTPProvider(POLYGON_RPC_URL))
        token = w3.eth.contract(address=_Web3.to_checksum_address(pUSD), abi=_ERC20_BALANCE_ABI)
        bal = token.functions.balanceOf(_Web3.to_checksum_address(DEPOSIT_WALLET)).call()
        return float(bal) / 1e6
    except Exception as e:
        logger.debug("get_clob_balance error: %s", e)
        return 0.0


async def get_clob_balance(private_key: str, api_key: str, api_secret: str, api_passphrase: str) -> float:
    """Solde USDC tradeable sur Polymarket (proxy wallet)."""
    try:
        return await asyncio.to_thread(_get_clob_balance_sync, private_key, api_key, api_secret, api_passphrase)
    except Exception as e:
        logger.debug("get_clob_balance error: %s", e)
        return 0.0


async def get_deposit_wallet_balance() -> float:
    """Retourne le solde pUSD du deposit wallet (pas de creds nécessaires)."""
    return await asyncio.to_thread(_get_clob_balance_sync, "", "", "", "")


# ── API credentials ───────────────────────────────────────────────────────────

def _create_creds_sync(private_key: str) -> dict:
    client = _make_client(private_key)
    creds  = client.create_or_derive_api_key()
    if not creds:
        raise Exception("Réponse vide de Polymarket — vérifie le proxy et la connexion.")
    return {
        "api_key":        creds.api_key,
        "api_secret":     creds.api_secret,
        "api_passphrase": creds.api_passphrase,
    }


async def create_api_creds(private_key: str) -> dict:
    return await asyncio.to_thread(_create_creds_sync, private_key)


# ── USDC transfer (withdraw) ──────────────────────────────────────────────────

_ERC20_TRANSFER_ABI = [{
    "inputs": [{"name": "to", "type": "address"}, {"name": "amount", "type": "uint256"}],
    "name": "transfer",
    "outputs": [{"name": "", "type": "bool"}],
    "stateMutability": "nonpayable",
    "type": "function",
}]


def _send_usdc_sync(private_key: str, to_address: str, amount_usdc: float) -> str:
    w3      = _make_w3()
    account = w3.eth.account.from_key(private_key)
    usdc    = w3.eth.contract(
        address=Web3.to_checksum_address(USDC_ADDRESS),
        abi=_ERC20_TRANSFER_ABI,
    )
    amount_raw = int(round(amount_usdc * 1_000_000))  # 6 decimals
    nonce = w3.eth.get_transaction_count(account.address, "pending")
    tx = usdc.functions.transfer(
        Web3.to_checksum_address(to_address), amount_raw
    ).build_transaction({"from": account.address, "nonce": nonce, "chainId": CHAIN_ID})
    signed   = account.sign_transaction(tx)
    tx_hash  = w3.eth.send_raw_transaction(signed.raw_transaction)
    w3.eth.wait_for_transaction_receipt(tx_hash, timeout=90)
    return "0x" + tx_hash.hex() if not tx_hash.hex().startswith("0x") else tx_hash.hex()


async def send_usdc(private_key: str, to_address: str, amount_usdc: float) -> str:
    """Transfer USDC on Polygon. Returns tx hash."""
    return await asyncio.to_thread(_send_usdc_sync, private_key, to_address, amount_usdc)


# ── Placement d'ordres avec slippage ─────────────────────────────────────────

def _place_order_sync(
    private_key: str, api_key: str, api_secret: str, api_passphrase: str,
    token_id: str, side: str, size_usdc: float, limit_price: float,
) -> dict:
    """Place un ordre FOK V2 via deposit wallet (POLY_1271)."""
    creds  = ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase)
    client = _make_client(private_key, creds)
    sdk_side = Side.BUY if side.upper() == "BUY" else Side.SELL
    order = client.create_market_order(MarketOrderArgsV2(
        token_id=token_id,
        amount=round(size_usdc, 2),  # BUY: amount = USDC à dépenser
        side=sdk_side,
        price=round(limit_price, 4),
        order_type=OrderType.FOK,
    ))
    return client.post_order(order, OrderType.FOK)


async def place_order(
    private_key: str, api_key: str, api_secret: str, api_passphrase: str,
    token_id: str, side: str, size_usdc: float, limit_price: float,
) -> dict:
    return await asyncio.to_thread(
        _place_order_sync,
        private_key, api_key, api_secret, api_passphrase,
        token_id, side, size_usdc, limit_price,
    )


# ── Ordre limite GTC (mode MAKER — reste au repos dans le carnet) ────────────

def _place_limit_order_sync(
    private_key: str, api_key: str, api_secret: str, api_passphrase: str,
    token_id: str, side: str, price: float, shares: float,
) -> dict:
    """Ordre limite GTC : reste dans le carnet jusqu'à fill ou annulation.
    Maker = zéro fee + rebates. `shares` = nombre de parts (pas des USDC)."""
    creds  = ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase)
    client = _make_client(private_key, creds)
    sdk_side = Side.BUY if side.upper() == "BUY" else Side.SELL
    order = client.create_order(OrderArgsV2(
        token_id=token_id,
        price=round(price, 4),
        size=round(shares, 2),
        side=sdk_side,
    ))
    return client.post_order(order, OrderType.GTC)


async def place_limit_order(
    private_key: str, api_key: str, api_secret: str, api_passphrase: str,
    token_id: str, side: str, price: float, shares: float,
) -> dict:
    return await asyncio.to_thread(
        _place_limit_order_sync,
        private_key, api_key, api_secret, api_passphrase,
        token_id, side, price, shares,
    )


def _get_open_orders_sync(
    private_key: str, api_key: str, api_secret: str, api_passphrase: str,
) -> list:
    creds  = ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase)
    client = _make_client(private_key, creds)
    return client.get_open_orders(OpenOrderParams())


async def get_open_orders(
    private_key: str, api_key: str, api_secret: str, api_passphrase: str,
) -> list:
    return await asyncio.to_thread(
        _get_open_orders_sync, private_key, api_key, api_secret, api_passphrase,
    )


# ── Annuler un ordre ─────────────────────────────────────────────────────────

def _cancel_order_sync(
    private_key: str, api_key: str, api_secret: str, api_passphrase: str,
    order_id: str,
) -> dict:
    creds  = ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase)
    client = _make_client(private_key, creds)
    # cancel_orders prend une liste de hashes — cancel_order exige un
    # OrderPayload complet (bug du dict découvert le 11/07)
    return client.cancel_orders([order_id])


async def cancel_order(
    private_key: str, api_key: str, api_secret: str, api_passphrase: str,
    order_id: str,
) -> dict:
    return await asyncio.to_thread(
        _cancel_order_sync,
        private_key, api_key, api_secret, api_passphrase, order_id,
    )


# ── Statut d'un ordre ────────────────────────────────────────────────────────

def _get_order_sync(
    private_key: str, api_key: str, api_secret: str, api_passphrase: str,
    order_id: str,
) -> dict:
    creds  = ApiCreds(api_key=api_key, api_secret=api_secret, api_passphrase=api_passphrase)
    client = _make_client(private_key, creds)
    return client.get_order(order_id)  # V2 client has same get_order signature


async def get_order_status(
    private_key: str, api_key: str, api_secret: str, api_passphrase: str,
    order_id: str,
) -> dict:
    return await asyncio.to_thread(
        _get_order_sync,
        private_key, api_key, api_secret, api_passphrase, order_id,
    )


# ── REST helpers (avec proxy aiohttp) ────────────────────────────────────────

async def _get(url: str, timeout: int = 10) -> Optional[dict | list]:
    proxy = PROXY_URL if PROXY_URL and not PROXY_URL.startswith("socks") else None
    try:
        async with _aiohttp_session() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=timeout), proxy=proxy) as r:
                if r.status == 200:
                    return await r.json()
    except Exception as e:
        logger.debug("GET %s error: %s", url, e)
    return None


async def get_best_price(token_id: str, side: str) -> Optional[float]:
    data = await _get(f"{CLOB_API}/price?token_id={token_id}&side={side}", timeout=8)
    if isinstance(data, dict):
        p = data.get("price")
        return float(p) if p else None
    return None


async def get_market_by_token(token_id: str) -> Optional[dict]:
    data = await _get(f"{GAMMA_API}/markets?clob_token_ids={token_id}&limit=1")
    if isinstance(data, list) and data:
        return data[0]
    return None


async def get_markets(limit: int = 20, offset: int = 0, query: str = "") -> list[dict]:
    params = f"?active=true&closed=false&limit={limit}&offset={offset}"
    if query:
        params += f"&_q={query}"
    data = await _get(f"{GAMMA_API}/markets{params}", timeout=12)
    return data if isinstance(data, list) else []


async def get_market_by_slug(slug: str) -> Optional[dict]:
    """Récupère un marché unique directement depuis l'API Gamma via son slug."""
    data = await _get(f"{GAMMA_API}/markets/slug/{slug}", timeout=8)
    if isinstance(data, dict):
        return data
    return None


DATA_API = "https://data-api.polymarket.com"


async def get_recent_trades_for_wallet(address: str, limit: int = 50) -> list[dict]:
    """Récupère l'activité récente d'un wallet (trades + redeems) via l'API publique."""
    data = await _get(f"{DATA_API}/activity?user={address}&limit={limit}", timeout=12)
    if isinstance(data, list):
        # Garde uniquement les TRADE (pas les REDEEM pour les stats)
        return data
    # Fallback ancien endpoint
    data2 = await _get(f"{DATA_API}/trades?user={address}&limit={limit}", timeout=12)
    return data2 if isinstance(data2, list) else []


async def get_wallet_positions(address: str) -> list[dict]:
    """Positions ouvertes d'un wallet (tente proxy wallet si nécessaire)."""
    data = await _get(f"{DATA_API}/positions?user={address}", timeout=12)
    if isinstance(data, list) and data:
        return data
    # Si vide, l'adresse est peut-être un EOA — récupère le proxyWallet depuis les trades
    trades = await get_recent_trades_for_wallet(address, limit=1)
    if trades:
        proxy = trades[0].get("proxyWallet", "")
        if proxy and proxy.lower() != address.lower():
            data2 = await _get(f"{DATA_API}/positions?user={proxy}", timeout=12)
            if isinstance(data2, list):
                return data2
    return []


async def _fetch_activity_batch(address: str, offsets: list[int]) -> list[dict]:
    """Fetch plusieurs pages en parallèle et les fusionne."""
    pages = await asyncio.gather(*[
        _get(f"{DATA_API}/activity?user={address}&limit=500&offset={off}", timeout=20)
        for off in offsets
    ], return_exceptions=True)
    result = []
    for p in pages:
        if isinstance(p, list):
            result.extend(p)
    return result


async def get_wallet_full_stats(address: str) -> dict:
    """
    PnL réalisé par période.
    Méthode : groupe les trades par conditionId, calcule (REDEEM+SELLs - BUYs)
    par position fermée, attribue le PnL à la période de clôture.
    Pagine jusqu'à couvrir 30j ou jusqu'à la limite de l'API (s'arrête dès page vide).
    Retourne aussi `data_days` = nombre de jours réellement couverts.
    """
    import time as _time
    from collections import defaultdict

    now     = _time.time()
    cut_30d = now - 30 * 86400
    cut_7d  = now - 7  * 86400
    cut_24h = now - 86400

    # ── Fetch pages jusqu'à 30j, s'arrête dès page vide ──────────────────────
    all_items: list[dict] = []
    page_size  = 500
    batch_size = 10   # pages en parallèle par batch
    max_pages  = 60   # cap absolu (30 000 items)

    for batch_start in range(0, max_pages, batch_size):
        offsets = [page_size * (batch_start + i) for i in range(batch_size)]
        pages   = await asyncio.gather(*[
            _get(f"{DATA_API}/activity?user={address}&limit={page_size}&offset={off}", timeout=20)
            for off in offsets
        ], return_exceptions=True)

        got_any = False
        for page in pages:
            if isinstance(page, list) and page:
                all_items.extend(page)
                got_any = True

        if not got_any:
            break   # toutes les pages de ce batch sont vides → fin des données

        # Stop si on a couvert 30j
        oldest = float(all_items[-1].get("timestamp") or now)
        if oldest < cut_30d:
            break

    if not all_items:
        return {"total_trades": 0, "pnl_24h": 0.0, "pnl_7d": 0.0, "pnl_30d": 0.0,
                "data_days": 0.0}

    data_oldest_ts = float(all_items[-1].get("timestamp") or now)
    data_days      = (now - data_oldest_ts) / 86400

    # ── Groupe par conditionId ────────────────────────────────────────────────
    by_cond: dict[str, dict] = defaultdict(lambda: {"buy": 0.0, "exits": []})
    trade_count = 0

    for t in all_items:
        ts      = float(t.get("timestamp") or 0)
        usdc    = float(t.get("usdcSize") or 0)
        if usdc == 0:
            usdc = float(t.get("size") or 0) * float(t.get("price") or 0)
        kind    = t.get("type", "")
        side    = (t.get("side") or "").upper()
        cond_id = t.get("conditionId") or t.get("asset") or ""

        if side == "BUY":
            by_cond[cond_id]["buy"] += usdc
            trade_count += 1
        elif side == "SELL":
            by_cond[cond_id]["exits"].append((ts, usdc))
            trade_count += 1
        elif kind == "REDEEM":
            by_cond[cond_id]["exits"].append((ts, usdc))

    # ── Calcul PnL par position fermée ────────────────────────────────────────
    pnl: dict[str, float] = {"24h": 0.0, "7d": 0.0, "30d": 0.0}

    for cond_id, data in by_cond.items():
        if not data["exits"] or data["buy"] == 0.0:
            continue   # position ouverte ou données incomplètes

        total_exit = sum(u for _, u in data["exits"])
        realized   = total_exit - data["buy"]
        close_ts   = max(ts for ts, _ in data["exits"])

        if close_ts >= cut_24h:
            pnl["24h"] += realized
        if close_ts >= cut_7d:
            pnl["7d"]  += realized
        if close_ts >= cut_30d:
            pnl["30d"] += realized

    return {
        "total_trades": trade_count,
        "pnl_24h":      pnl["24h"],
        "pnl_7d":       pnl["7d"],
        "pnl_30d":      pnl["30d"],
        "data_days":    data_days,   # couverture réelle des données
    }


# Compat alias for existing callers
async def get_wallet_stats(address: str) -> dict:
    return await get_wallet_full_stats(address)


def _keyword_matches(question: str, keyword: str) -> bool:
    """
    Vérifie que le keyword correspond à la question.
    Essaie d'abord la correspondance exacte de la phrase, puis par mots.
    Mots courts (≥2 chars) inclus — important pour '5m', 'up', etc.
    """
    q_lower  = question.lower()
    kw_lower = keyword.lower()
    if kw_lower in q_lower:
        return True
    # Supprime seulement les mots vides d'1 char (prepositions, etc.)
    words = [w for w in kw_lower.split() if len(w) >= 2]
    return bool(words) and all(w in q_lower for w in words)


async def _clob_accepting_markets(keyword: str, max_pages: int = 20) -> list[dict]:
    """
    Parcourt le CLOB API (curseur) et retourne les marchés qui :
    - acceptent des ordres (accepting_orders=True)
    - dont la question correspond au keyword
    L'API Gamma/_q est cassée donc on utilise cette approche.
    """
    cursor = ""
    matched: list[dict] = []
    for _ in range(max_pages):
        url = f"{CLOB_API}/markets?limit=500" + (f"&next_cursor={cursor}" if cursor else "")
        data = await _get(url, timeout=20)
        if not isinstance(data, dict):
            break
        for m in data.get("data", []):
            if not m.get("accepting_orders"):
                continue
            question = m.get("question") or ""
            if _keyword_matches(question, keyword):
                matched.append(m)
        cursor = data.get("next_cursor", "")
        if not cursor or cursor == "LTE=":
            break
    return matched


async def get_series_current_markets(series_slug: str) -> list[dict]:
    """
    Récupère le marché actif courant d'une série Polymarket.

    Stratégie :
    1. Détecte le préfixe de slug et l'intervalle (ex: "btc-updown-5m", 300s)
       depuis les events de la série.
    2. Construit directement les slugs candidates à partir de l'heure courante
       (fenêtre courante ± 1 intervalle) → accès O(1) au marché en cours.
    3. Fallback : parcourt les events de la série (plus anciens → plus récents)
       et retourne le premier accepting_orders trouvé.
    """
    import time as _time
    import re   # _re déprécié ou alias inutile, 're' natif utilisé proprement

    series_data = await _get(f"{GAMMA_API}/series?slug={series_slug}", timeout=12)
    if not isinstance(series_data, list) or not series_data:
        return []

    events = [e for e in series_data[0].get("events", []) if isinstance(e, dict)]
    if not events:
        return []

    # ── Étape 1 : détection du préfixe et de l'intervalle ────────────────────
    slug_prefix: str | None = None
    interval: int = 300  # défaut 5m

    ts_list = []
    for e in events:
        m = re.match(r'^(.+?)-(\d{9,11})$', e.get("slug", ""))
        if m:
            slug_prefix = m.group(1)
            ts_list.append(int(m.group(2)))

    if len(ts_list) >= 2:
        ts_sorted = sorted(set(ts_list))
        diffs = [ts_sorted[i+1] - ts_sorted[i] for i in range(len(ts_sorted) - 1)]
        interval = min(diffs)   # intervalle réel (300 pour 5m, 900 pour 15m…)

    # ── Étape 2 : construction directe par timestamp courant ──────────────────
    async def _fetch_event_slug(event_slug: str) -> list[dict]:
        event_full = await _get(f"{GAMMA_API}/events?slug={event_slug}", timeout=10)
        if not isinstance(event_full, list) or not event_full:
            return []
        out = []
        for m in event_full[0].get("markets", []):
            cid = m.get("conditionId", "")
            if not cid:
                continue
            clob_m = await _get(f"{CLOB_API}/markets/{cid}", timeout=8)
            if not isinstance(clob_m, dict) or not clob_m.get("accepting_orders"):
                continue
            question = clob_m.get("question", "")[:80]
            tokens = clob_m.get("tokens") or []
            # Vérifie qu'il y a vraiment un orderbook actif (prix > 0)
            if tokens:
                try:
                    probe = await _get(f"{CLOB_API}/price?token_id={tokens[0].get('token_id','')}&side=BUY", timeout=5)
                    if not probe or float(probe.get("price", 0)) <= 0:
                        continue   # marché en règlement, plus de liquidité
                except Exception:
                    continue
            for t in tokens[:2]:
                out.append({
                    "condition_id": cid,
                    "token_id":     str(t.get("token_id", "")),
                    "outcome":      t.get("outcome", "?"),
                    "question":     question,
                })
        return out

    if slug_prefix:
        now = int(_time.time())
        base = (now // interval) * interval
        # Essaie : fenêtre précédente, courante, suivante
        for offset in (0, interval, -interval):  # courant d'abord, puis suivant, puis précédent
            ts = base + offset
            found = await _fetch_event_slug(f"{slug_prefix}-{ts}")
            if found:
                return found

    # ── Étape 3 : fallback série (oldest accepting first) ────────────────────
    for event in events:
        found = await _fetch_event_slug(event.get("slug", ""))
        if found:
            return found

    return []


# Mapping : (conditions OR-groupes, series slugs)
_SERIES_MAP: list[tuple[list[list[str]], list[str]]] = [
    # BTC — la correspondance "5m" doit venir AVANT la règle générique 15m
    ([["bitcoin", "btc"], ["5m", "5 m", "5min"]],                 ["btc-up-or-down-5m"]),
    ([["bitcoin", "btc"], ["15m", "15 m", "15min"]],                 ["btc-up-or-down-15m"]),
    ([["bitcoin", "btc"], ["hour", "hourly", "1h"]],                 ["btc-up-or-down-hourly"]),
    ([["bitcoin", "btc"], ["daily", "day", "1d"], ["up", "down"]],   ["btc-up-or-down-daily"]),
    ([["bitcoin", "btc"], ["weekly", "week", "1w"], ["up", "down"]], ["bitcoin-up-or-down-weekly"]),
    # ETH
    ([["ethereum", "eth"], ["5m", "5 m", "5min"]],                   ["eth-up-or-down-5m"]),
    ([["ethereum", "eth"], ["15m", "15 m", "15min"]],                ["eth-up-or-down-15m"]),
    ([["ethereum", "eth"], ["hour", "hourly", "1h"]],                ["eth-up-or-down-hourly"]),
    ([["ethereum", "eth"], ["daily", "day", "1d"], ["up", "down"]],  ["eth-up-or-down-daily"]),
    # Autres
    ([["solana", "sol"],   ["hour", "hourly", "1h"]],                ["solana-up-or-down-hourly"]),
    ([["xrp"],             ["hour", "hourly", "1h"]],                ["xrp-up-or-down-hourly"]),
    ([["xrp"],             ["daily", "day", "1d"]],                  ["xrp-up-or-down-daily"]),
]


def _find_series_for_keyword(keyword: str) -> list[str]:
    """Retourne les series slugs correspondant au keyword (matching souple)."""
    kw = keyword.lower()
    matches = []
    for conditions, slugs in _SERIES_MAP:
        if all(any(syn in kw for syn in group) for group in conditions):
            matches.extend(slugs)
    return matches


async def scan_markets_for_strategy(keyword: str, trigger_price: float, trigger_dir: str = 'above') -> list[dict]:
    """
    Cherche les marchés ouverts correspondant au keyword.
    """
    hits: list[dict] = []

    # ── Source 1 : Séries Polymarket (marchés récurrents) ─────────────────────
    series_slugs = _find_series_for_keyword(keyword)
    if series_slugs:
        for slug in series_slugs:
            tokens_meta = await get_series_current_markets(slug)
            price_tasks = [get_best_price(t["token_id"], "BUY") for t in tokens_meta]
            prices      = await asyncio.gather(*price_tasks, return_exceptions=True)
            seen_tids   = {h["token_id"] for h in hits}
            for meta, price in zip(tokens_meta, prices):
                if isinstance(price, Exception) or price is None:
                    continue
                if meta["token_id"] in seen_tids:
                    continue
                if trigger_dir == 'above':
                    if price < trigger_price:
                        continue
                else:  # under
                    if price > trigger_price:
                        continue
                if price <= 0 or price >= 1:
                    continue
                hits.append({**meta, "price": price})
                seen_tids.add(meta["token_id"])

    # ── Source 2 : CLOB API générique (fallback pour autres marchés) ──────────
    if not hits:
        clob_markets = await _clob_accepting_markets(keyword)
        price_tasks  = []
        meta_list: list[tuple] = []
        for m in clob_markets:
            cid      = m.get("condition_id", "")
            question = (m.get("question") or "")[:80]
            for t in (m.get("tokens") or [])[:2]:
                tid = str(t.get("token_id", ""))
                if tid:
                    price_tasks.append(get_best_price(tid, "BUY"))
                    meta_list.append((cid, tid, t.get("outcome", "?"), question))

        prices = await asyncio.gather(*price_tasks, return_exceptions=True)
        for (cid, tid, outcome, question), price in zip(meta_list, prices):
            if isinstance(price, Exception) or price is None:
                continue
            if trigger_dir == 'above':
                if price < trigger_price:
                    continue
            else:  # under
                if price > trigger_price:
                    continue
            if price <= 0 or price >= 1:
                continue
            hits.append({
                "condition_id": cid,
                "token_id":     tid,
                "outcome":      outcome,
                "price":        price,
                "question":     question,
            })

    return hits