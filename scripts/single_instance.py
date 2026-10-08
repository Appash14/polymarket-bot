"""Garde anti double-instance — fix du bug de doublon de trade du 10/07/2026.

Diagnostic : les trades en double (2 ordres réels à 10ms-3.6s d'écart, parfois
UP puis DOWN à 35s d'écart dans la même fenêtre — trades #140/141) ne venaient
PAS d'un bug de concurrence asyncio. Un seul processus ne peut pas retrader une
fenêtre : `traded_this_window` le bloque, lock ou pas. La seule explication
cohérente avec toutes les observations : DEUX instances du script tournaient en
parallèle (ancien processus jamais tué avant relance). Chaque instance écoutait
le même flux Binance et tradait indépendamment — d'où des ordres quasi
simultanés (jitter réseau 10-800ms) et l'inutilité du Lock (par-processus).

Ce module rend le problème impossible :
1. Lockfile PID exclusif (O_CREAT|O_EXCL) — la 2e instance refuse de démarrer.
2. Si le lockfile existe mais que le PID est mort (crash), il est recyclé.

Usage, tout en haut du main() de n'importe quel bot :

    from single_instance import acquire_lock
    acquire_lock("stale_quote_bot")   # raise SystemExit si déjà en cours
"""

import atexit
import os
import sys
from pathlib import Path

LOCK_DIR = Path(__file__).parent.parent / "data"


def _pid_alive(pid: int) -> bool:
    """Vérifie qu'un PID est vivant (Windows + Unix)."""
    if sys.platform == "win32":
        import ctypes
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        h = ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if not h:
            return False
        # le handle peut s'ouvrir sur un processus zombie : vérifie le code de sortie
        code = ctypes.c_ulong()
        ok = ctypes.windll.kernel32.GetExitCodeProcess(h, ctypes.byref(code))
        ctypes.windll.kernel32.CloseHandle(h)
        STILL_ACTIVE = 259
        return bool(ok) and code.value == STILL_ACTIVE
    else:
        try:
            os.kill(pid, 0)
            return True
        except (ProcessLookupError, PermissionError):
            return False


def running_pid(name: str):
    """PID de l'instance en cours, ou None si libre. Ne PREND PAS le lock —
    sert au contrôle en amont (bot.py) pour refuser proprement AVANT de lancer
    quoi que ce soit (sinon les collecteurs démarrent puis meurent salement)."""
    lock_path = LOCK_DIR / f"{name}.pid"
    if not lock_path.exists():
        return None
    try:
        pid = int(lock_path.read_text().strip())
    except (ValueError, OSError):
        return None
    return pid if _pid_alive(pid) else None


def acquire_lock(name: str) -> None:
    """Prend le lock exclusif `name` ou quitte avec un message explicite."""
    LOCK_DIR.mkdir(parents=True, exist_ok=True)
    lock_path = LOCK_DIR / f"{name}.pid"

    if lock_path.exists():
        try:
            old_pid = int(lock_path.read_text().strip())
        except (ValueError, OSError):
            old_pid = None
        if old_pid and _pid_alive(old_pid):
            raise SystemExit(
                f"REFUS DE DÉMARRER : une instance de '{name}' tourne déjà (PID {old_pid}).\n"
                f"C'est exactement la cause des trades en double du 10/07/2026.\n"
                f"Tue-la d'abord (taskkill /PID {old_pid} /F  ou  kill {old_pid}), "
                f"ou supprime {lock_path} si tu es certain qu'elle est morte."
            )
        # PID mort : lock orphelin, on le recycle
        lock_path.unlink(missing_ok=True)

    fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    os.write(fd, str(os.getpid()).encode())
    os.close(fd)

    def _release():
        try:
            if lock_path.exists() and lock_path.read_text().strip() == str(os.getpid()):
                lock_path.unlink()
        except OSError:
            pass

    atexit.register(_release)
