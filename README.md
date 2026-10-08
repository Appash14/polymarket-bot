# polymarket-bot

Un programme Python qui suit en temps réel les marchés Polymarket de prix du Bitcoin à cinq minutes, enregistre les données et teste une règle de décision automatique, avec un tableau de bord dans le terminal et sur Telegram.

## Pourquoi

Je voulais apprendre à brancher un programme sur des flux de données en direct (WebSocket), à stocker des centaines d'événements par seconde sans perte, et à vérifier une idée avec des chiffres plutôt qu'à l'intuition. Les marchés de prédiction s'y prêtent : tout est public, horodaté, et le résultat de chaque fenêtre de cinq minutes est connu.

Les essais ont été faits avec de très petites sommes. Ce qui compte dans ce projet, c'est la partie technique : la collecte, la base de données, l'analyse et les tableaux de bord.

## Ce que fait le programme

`python bot.py` lance trois tâches en parallèle :

1. un collecteur Polymarket : abonnement WebSocket au carnet d'ordres de la fenêtre en cours, enregistrement dans SQLite (environ 700 événements par seconde traités) ;
2. un collecteur de bougies Binance (BTC/USDT, cinq minutes) ;
3. le moteur de décision, avec un tableau de bord qui se réécrit en place dans le terminal (bibliothèque `rich`) et un menu de réglages sur Telegram, réservé au propriétaire du bot.

## Structure

```
bot.py                  point d'entrée unique
config.py               lecture de la configuration (.env)
polymarket.py           client Polymarket (CLOB), Binance et Gamma
scripts/
  copy99_bot.py         moteur de décision et tableau de bord Telegram
  collectors.py         les deux collecteurs de données
  terminal_ui.py        tableau de bord du terminal
  telegram_ui.py        interface Telegram (tableau de bord, réglages)
  single_instance.py    verrou contre une double instance
```

Les données (`data/`) ne sont pas versionnées.

## Stack

Python 3, asyncio, aiohttp (HTTP et WebSocket), SQLite (aiosqlite), web3.py, client CLOB Polymarket, rich, API Telegram.

## Lancer

```bash
pip install -r requirements.txt
cp .env.example .env    # remplir les valeurs
python bot.py
```

## Avertissement

Projet d'apprentissage. Ce n'est pas un conseil financier et ce dépôt n'incite ni à parier ni à spéculer : les paris en ligne font perdre de l'argent à la grande majorité des gens. Le code est publié pour sa partie technique (collecte de données en temps réel, analyse, interfaces). Polymarket n'est pas accessible depuis tous les pays : respectez la loi du vôtre.
