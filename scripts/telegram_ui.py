"""UI Telegram propre pour edge_trader — dashboard vivant + boutons + auto-nettoyage.

Principes :
  - UN message dashboard, édité en place toutes les DASH_INTERVAL secondes,
    et reposté tout en bas dès qu'une notification l'a fait remonter
    (l'ancien est supprimé → jamais deux dashboards).
  - Boutons inline : ⏸/▶️ (toggle), 🔄 actualiser.
  - Les notifications (fills, résolutions…) portent un TTL et sont supprimées
    automatiquement une fois périmées — le chat reste propre, le dashboard
    porte l'état complet en permanence.
  - Limite Telegram : un bot ne peut supprimer QUE ses propres messages
    (< 48h). Les messages de l'utilisateur restent.
"""

import asyncio
import logging
import time

import aiohttp

logger = logging.getLogger("tgui")

DASH_INTERVAL   = 15        # secondes entre deux rafraîchissements du dashboard.
                            # 15s (pas 5s) : ~240 éditions/h au lieu de 720 -> évite les
                            # flood-blocks Telegram à répétition (incidents 8600s / 18494s).
GC_INTERVAL     = 30        # cadence du nettoyage des messages périmés
DEFAULT_TTL     = 6 * 3600  # durée de vie par défaut d'une notification
TRANSIENT_TTL   = 90        # confirmations éphémères (pause, reprise…)


class TelegramUI:
    def __init__(self, token: str, chat_id, build_dashboard, on_action, viewer_usernames=None,
                 apply_setting=None, get_settings=None):
        """build_dashboard : coroutine -> str (texte du dashboard)
        on_action : coroutine (action: str) -> str|None (réponse éphémère)
        viewer_usernames : usernames Telegram autorisés en LECTURE SEULE (dashboard
        poussé toutes les 15s, aucune commande possible).
        apply_setting : coroutine (key, raw) -> str (applique un réglage, renvoie la confirmation)
        get_settings : callable -> dict (réglages courants, pour afficher les valeurs dans le menu)."""
        self._api_base = f"https://api.telegram.org/bot{token}"
        self.chat_id = chat_id
        self.build_dashboard = build_dashboard
        self.on_action = on_action
        self.apply_setting = apply_setting
        self.get_settings = get_settings
        self._menu_msg = None            # id du message-menu ⚙️ (séparé du dashboard)
        self._pending = None             # clé de réglage en attente d'une saisie texte
        self.viewer_usernames = {u.lstrip("@").lower() for u in (viewer_usernames or [])}
        self.viewers = {}                # chat_id -> {"msg": id, "text": str}
        self._dash_msg_id: int | None = None
        self._dash_text: str = ""
        self._dash_dirty_pos = False     # une notif est passée sous le dashboard
        self._ttl_registry: list[tuple[int, float]] = []   # (message_id, expire_ts)
        self._offset = 0
        self._sess: aiohttp.ClientSession | None = None
        self._refresh_event = asyncio.Event()
        self._flood_until = 0.0          # respect du retry_after Telegram (429)

    # ── API bas niveau ────────────────────────────────────────────────────────

    async def _api(self, method: str, **params):
        # respecte un éventuel flood-control en cours : ne PAS re-cogner (sinon
        # Telegram escalade le retry_after — c'est ce qui a bloqué le dashboard 13/07)
        if time.time() < self._flood_until:
            return None
        try:
            async with self._sess.post(f"{self._api_base}/{method}", json=params,
                                       timeout=aiohttp.ClientTimeout(total=15)) as r:
                data = await r.json()
            if not data.get("ok"):
                if data.get("error_code") == 429:
                    ra = (data.get("parameters") or {}).get("retry_after", 30)
                    self._flood_until = time.time() + ra + 1
                    logger.warning("Telegram flood-control: pause %ss (%s)", ra, method)
                    return None
                desc = data.get("description", "")
                if "message is not modified" not in desc:
                    logger.debug("%s KO: %s", method, desc)
                return None
            return data.get("result")
        except Exception as e:
            logger.debug("%s: %s", method, e)
            return None

    def _keyboard(self, paused: bool) -> dict:
        toggle = {"text": "▶️ Reprendre", "callback_data": "resume"} if paused \
            else {"text": "⏸ Pause", "callback_data": "pause"}
        return {"inline_keyboard": [[
            toggle,
            {"text": "⚙️ Réglages", "callback_data": "settings"},
            {"text": "🔄 Actualiser", "callback_data": "refresh"},
        ]]}

    # ── Menu Réglages (message séparé, ne pas réutiliser le dashboard) ─────────
    _MENU_ACTIONS = {"settings", "menu_bet", "menu_cond", "menu_tp", "menu_close",
                     "bet_fixed", "bet_pct", "tp_sell99", "tp_hold",
                     "cond_low", "cond_trig", "cond_tmin", "cond_tmax"}

    def _menu_kb(self, name: str) -> dict:
        s = self.get_settings() if self.get_settings else {}
        if name == "menu_bet":
            mode = s.get("bet_mode", "pct")
            val = s.get("bet_value", 0)
            cur = f"actuel : {'$'+format(val,'.2f') if mode=='fixed' else format(val,'.2f')+'%'}"
            rows = [[{"text": "💵 FIXED ($)", "callback_data": "bet_fixed"},
                     {"text": "％ POURCENT", "callback_data": "bet_pct"}],
                    [{"text": f"⬅️ Retour  ·  {cur}", "callback_data": "settings"}]]
        elif name == "menu_cond":
            rows = [[{"text": f"≤ armement ({s.get('low',0):.2f})", "callback_data": "cond_low"},
                     {"text": f"≥ achat ({s.get('trigger',0):.2f})", "callback_data": "cond_trig"}],
                    [{"text": f"⏱ min ({s.get('tmin',0)}s)", "callback_data": "cond_tmin"},
                     {"text": f"⏱ max ({s.get('tmax',0)}s)", "callback_data": "cond_tmax"}],
                    [{"text": "⬅️ Retour", "callback_data": "settings"}]]
        elif name == "menu_tp":
            cur = s.get("tp_mode", "hold")
            rows = [[{"text": ("✅ " if cur == "sell99" else "") + "💸 Vendre à 0.99",
                      "callback_data": "tp_sell99"},
                     {"text": ("✅ " if cur == "hold" else "") + "⏳ Attendre résolution",
                      "callback_data": "tp_hold"}],
                    [{"text": "⬅️ Retour", "callback_data": "settings"}]]
        else:  # racine
            tp = "0.99" if s.get("tp_mode", "hold") == "sell99" else "résolution"
            rows = [[{"text": "💰 Bet size", "callback_data": "menu_bet"},
                     {"text": "🎯 Conditions", "callback_data": "menu_cond"}],
                    [{"text": f"💸 Take profit ({tp})", "callback_data": "menu_tp"}],
                    [{"text": "✖️ Fermer", "callback_data": "menu_close"}]]
        return {"inline_keyboard": rows}

    _MENU_TITLE = {
        "settings":  "⚙️ Réglages — choisis une catégorie :",
        "menu_bet":  "💰 Bet size — choisis le mode, puis écris la valeur :",
        "menu_cond": "🎯 Conditions — choisis un paramètre, puis écris la valeur :",
        "menu_tp":   "💸 Take profit — vendre à 0.99 ou tenir jusqu'à la résolution ?",
    }

    async def _menu(self, action: str, cq_id: str):
        # boutons feuilles : arment une saisie texte
        prompts = {
            "cond_low":  "Écris le nouveau seuil d'armement ≤ (ex: 0.38).",
            "cond_trig": "Écris le nouveau seuil d'achat ≥ (ex: 0.53).",
            "cond_tmin": "Écris le temps restant MIN en secondes (ex: 8).",
            "cond_tmax": "Écris le temps restant MAX en secondes (ex: 240).",
        }
        if action == "menu_close":
            if self._menu_msg:
                await self._api("deleteMessage", chat_id=self.chat_id, message_id=self._menu_msg)
                self._menu_msg = None
            self._pending = None
            await self._api("answerCallbackQuery", callback_query_id=cq_id)
            return
        if action in ("bet_fixed", "bet_pct"):
            mode = "fixed" if action == "bet_fixed" else "pct"
            msg = await self.apply_setting("bet_mode", mode) if self.apply_setting else ""
            self._pending = "bet_value"
            await self._api("answerCallbackQuery", callback_query_id=cq_id, text=(msg or "")[:190])
            return
        if action in ("tp_sell99", "tp_hold"):
            # choix direct (aucune saisie) -> on ré-affiche le menu TP coché
            mode = "sell99" if action == "tp_sell99" else "hold"
            msg = await self.apply_setting("tp_mode", mode) if self.apply_setting else ""
            self._pending = None
            await self._api("answerCallbackQuery", callback_query_id=cq_id, text=(msg or "")[:190])
            action = "menu_tp"          # retombe sur le rendu du menu ci-dessous
            cq_id = None
        if action in prompts:
            self._pending = {"cond_low": "low", "cond_trig": "trigger",
                             "cond_tmin": "tmin", "cond_tmax": "tmax"}[action]
            await self._api("answerCallbackQuery", callback_query_id=cq_id,
                            text=prompts[action][:190])
            return
        # navigation (settings / menu_bet / menu_cond / menu_tp) : (re)poser le message-menu
        if cq_id:                       # None si on a déjà répondu (choix TP)
            await self._api("answerCallbackQuery", callback_query_id=cq_id)
        title = self._MENU_TITLE.get(action, self._MENU_TITLE["settings"])
        kb = self._menu_kb(action)
        if self._menu_msg:
            r = await self._api("editMessageText", chat_id=self.chat_id, message_id=self._menu_msg,
                                text=title, reply_markup=kb, disable_web_page_preview=True)
            if r is not None:
                return
        r = await self._api("sendMessage", chat_id=self.chat_id, text=title,
                            reply_markup=kb, disable_web_page_preview=True)
        if r:
            self._menu_msg = r["message_id"]

    # ── Notifications ─────────────────────────────────────────────────────────

    async def notify(self, text: str, ttl: float = DEFAULT_TTL):
        """Envoie une notification ; elle sera auto-supprimée après `ttl`
        secondes, et le dashboard sera reposté en dessous au prochain tick."""
        res = await self._api("sendMessage", chat_id=self.chat_id, text=text,
                              disable_web_page_preview=True)
        if res and ttl:
            self._ttl_registry.append((res["message_id"], time.time() + ttl))
        self._dash_dirty_pos = True
        self._refresh_event.set()

    # ── Dashboard ─────────────────────────────────────────────────────────────

    async def _render_dashboard(self, paused: bool):
        if time.time() < self._flood_until:
            return                       # flood-control actif : ne rien pousser (anti-spam)
        try:
            text = await self.build_dashboard()
        except Exception as e:
            logger.warning("build_dashboard: %s", e)
            return
        kb = self._keyboard(paused)
        if self._dash_msg_id and not self._dash_dirty_pos:
            if text == self._dash_text:
                return
            res = await self._api("editMessageText", chat_id=self.chat_id,
                                  message_id=self._dash_msg_id, text=text,
                                  reply_markup=kb, disable_web_page_preview=True)
            if res is not None:
                self._dash_text = text
                return
            # édition impossible (message supprimé ?) → repost
        # repost tout en bas
        if self._dash_msg_id:
            await self._api("deleteMessage", chat_id=self.chat_id,
                            message_id=self._dash_msg_id)
        res = await self._api("sendMessage", chat_id=self.chat_id, text=text,
                              reply_markup=kb, disable_web_page_preview=True)
        if res:
            self._dash_msg_id = res["message_id"]
            self._dash_text = text
            self._dash_dirty_pos = False
        # ── spectateurs (lecture seule : même dashboard, SANS boutons) ──
        for cid, v in list(self.viewers.items()):
            if v.get("msg") and v.get("text") == text:
                continue
            ok = False
            if v.get("msg"):
                r = await self._api("editMessageText", chat_id=cid, message_id=v["msg"],
                                    text=text, disable_web_page_preview=True)
                if r is not None:
                    v["text"] = text; ok = True
            if not ok:
                r = await self._api("sendMessage", chat_id=cid, text=text,
                                   disable_web_page_preview=True)
                if r:
                    v["msg"] = r["message_id"]; v["text"] = text; ok = True
            v["fails"] = 0 if ok else v.get("fails", 0) + 1
            if v["fails"] >= 5:                     # spectateur mort -> on le retire
                logger.warning("spectateur %s retiré (%d échecs)", cid, v["fails"])
                self.viewers.pop(cid, None)

    async def _dashboard_loop(self, get_paused):
        while True:
            try:
                await asyncio.wait_for(self._refresh_event.wait(), timeout=DASH_INTERVAL)
            except asyncio.TimeoutError:
                pass
            self._refresh_event.clear()
            await self._render_dashboard(get_paused())

    # ── Nettoyage auto ────────────────────────────────────────────────────────

    async def _gc_loop(self):
        while True:
            await asyncio.sleep(GC_INTERVAL)
            now = time.time()
            keep = []
            for mid, exp in self._ttl_registry:
                if now >= exp:
                    await self._api("deleteMessage", chat_id=self.chat_id, message_id=mid)
                else:
                    keep.append((mid, exp))
            self._ttl_registry = keep

    # ── Updates (boutons + commandes texte) ───────────────────────────────────

    async def _updates_loop(self):
        # purge le backlog
        res = await self._api("getUpdates", timeout=0, offset=-1)
        if res:
            self._offset = res[-1]["update_id"] + 1
        while True:
            try:
                async with self._sess.get(
                    f"{self._api_base}/getUpdates",
                    params={"timeout": 25, "offset": self._offset},
                    timeout=aiohttp.ClientTimeout(total=35),
                ) as r:
                    data = await r.json()
            except Exception:
                await asyncio.sleep(5)
                continue
            if not data.get("ok"):
                logger.warning("getUpdates KO: %s — autre process sur le même token ?",
                               data.get("description"))
                await asyncio.sleep(10)
                continue
            for u in data.get("result", []):
                self._offset = u["update_id"] + 1
                await self._handle_update(u)

    async def _handle_update(self, u: dict):
        # Boutons
        cq = u.get("callback_query")
        if cq:
            if str(cq.get("message", {}).get("chat", {}).get("id", "")) != str(self.chat_id):
                return
            action = cq.get("data", "")
            if action in self._MENU_ACTIONS:          # menu ⚙️ Réglages (message séparé)
                try:
                    await self._menu(action, cq["id"])
                except Exception as e:
                    logger.error("menu %s: %s", action, e)
                    await self._api("answerCallbackQuery", callback_query_id=cq["id"])
                return
            reply = None
            try:
                reply = await self.on_action(action)
            except Exception as e:
                logger.error("action %s: %s", action, e)
            await self._api("answerCallbackQuery", callback_query_id=cq["id"],
                            text=(reply or "")[:190])
            self._refresh_event.set()
            return
        # Commandes texte (compat : /pause /resume /status /trades)
        msg = u.get("message") or {}
        cid = msg.get("chat", {}).get("id")
        if str(cid) != str(self.chat_id):
            # pas le propriétaire -> spectateur autorisé ? (LECTURE SEULE, aucune commande)
            uname = ((msg.get("from") or {}).get("username") or "").lower()
            if cid is not None and uname in self.viewer_usernames and cid not in self.viewers:
                self.viewers[cid] = {"msg": None, "text": ""}
                await self._api("sendMessage", chat_id=cid,
                                text="👀 Mode spectateur activé — tu recevras le dashboard "
                                     "en direct, en lecture seule.",
                                disable_web_page_preview=True)
                self._refresh_event.set()
            return
        text = (msg.get("text") or "").strip().lower()
        if not text:
            return
        if self._pending and self.apply_setting:          # saisie d'un réglage en attente
            key, self._pending = self._pending, None
            conf = await self.apply_setting(key, text)
            await self.notify(conf or "❌ Valeur invalide.", ttl=TRANSIENT_TTL)
            self._refresh_event.set()
            return
        cmd = text.split()[0].split("@")[0].lstrip("/")
        if cmd in ("pause", "resume", "start", "status", "trades", "refresh"):
            reply = None
            try:
                reply = await self.on_action("resume" if cmd == "start" else cmd)
            except Exception as e:
                logger.error("commande /%s: %s", cmd, e)
            if reply:
                await self.notify(reply, ttl=TRANSIENT_TTL)
            self._refresh_event.set()

    # ── Entrée ────────────────────────────────────────────────────────────────

    async def run(self, get_paused):
        async with aiohttp.ClientSession() as sess:
            self._sess = sess
            await self._api("setMyCommands", commands=[
                {"command": "status", "description": "Reposter le dashboard en bas"},
                {"command": "pause", "description": "Suspendre les entrées"},
                {"command": "resume", "description": "Reprendre le trading"},
            ])
            await asyncio.gather(
                self._dashboard_loop(get_paused),
                self._updates_loop(),
                self._gc_loop(),
            )
