# Projet 6 : Nearby-Driver Locator (Redis)

Projet NoSQL, UIR, 4e année Big Data & IA.
Groupe 6 : KONE Cheick Aboubacar, KARTIT Aymane, IRAQUI HOUSSAINI Ghali, GUENNOUN Yassine.

Couche clé-valeur d'un service de VTC : suivi de la position des chauffeurs en temps réel,
recherche des chauffeurs libres les plus proches d'un client, réservation atomique,
disparition automatique des chauffeurs inactifs et compteurs de demandes par zone et par heure.

## Prérequis

- Linux, ou Windows avec WSL2 (Ubuntu)
- Python 3.10 ou plus
- Redis 6.2 ou plus (la commande `GEOSEARCH` n'existe pas avant). Testé avec Redis 8.0.5.
- 8 Go de RAM suffisent

## 1. Lancer Redis

Avec Redis installé sur la machine :

```bash
sudo service redis-server start
redis-cli ping            # doit répondre PONG
```

Ou avec Docker, en limitant la mémoire :

```bash
docker run -d --name redis-p6 -p 6379:6379 --memory=1g redis:8
```

## 2. Installer l'environnement Python

```bash
cd projet6
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 3. Récupérer les données

1. Créer un compte sur Kaggle et rejoindre la compétition
   [NYC Taxi Trip Duration](https://www.kaggle.com/c/nyc-taxi-trip-duration) (onglet Data, « Join Competition »).
2. Télécharger `train.zip` et le décompresser.
3. Placer `train.csv` dans `data/` :

```bash
mkdir -p data
cp /chemin/vers/train.csv data/
wc -l data/train.csv      # 1 458 645 lignes avec l'en-tête
```

Les données restent soumises aux règles de la compétition Kaggle et ne sont pas incluses dans le dépôt.

## 4. Reproduire le projet

Toutes les commandes se lancent depuis le dossier `projet6`, avec l'environnement activé.

| Étape | Commande | Durée indicative | Sortie |
|---|---|---|---|
| Nettoyage, sous-ensemble, chauffeurs simulés | `python src/prepare_data.py` | moins d'une minute | `data/trips_clean.csv`, `data/quality_report.txt` |
| Démonstration des 12 requêtes | `python src/redis_model.py` | quelques secondes | affichage à l'écran |
| Simulation d'une journée | `python src/simulate.py` | 1 à 2 min | `results/simulation_fleet1_24h.json` |
| Simulation avec une flotte x10 | `python src/simulate.py --fleet 10 --hours 6` | moins d'une minute | `results/simulation_fleet10_6h.json` |
| Benchmark | `python src/benchmark.py` | quelques minutes | `results/benchmark.json`, `results/fig1..fig3*.png` |

Chaque script supprime au démarrage ses propres clés (`p6:*`, `demo:*` ou `bench:*`) et ne touche pas aux autres clés de Redis.
Les tirages aléatoires utilisent une graine fixe (`--seed 42`), donc deux lancements donnent les mêmes données.
Seules les latences changent d'une machine à l'autre.

Options utiles :

- `prepare_data.py --start 2016-03-07 --end 2016-03-14` : période gardée (fin exclue)
- `simulate.py --day 2016-03-07 --hours 24 --fleet 1 --speed 0` : jour rejoué, durée, multiplicateur de flotte, vitesse (0 = le plus vite possible, 60 = une minute simulée par seconde réelle)
- `benchmark.py --queries 1000` : nombre de recherches par mesure
- variable d'environnement `REDIS_PORT` si Redis n'écoute pas sur 6379

## Organisation du code

```
projet6/
├── data/                  données (non versionnées)
├── results/               mesures JSON et graphiques
├── src/
│   ├── prepare_data.py    nettoyage, sous-ensemble d'une semaine, attribution des chauffeurs
│   ├── redis_model.py     modèle de clés, scripts Lua, requêtes Q1 à Q12, démonstration
│   ├── simulate.py        rejeu des courses avec des chauffeurs en mouvement
│   └── benchmark.py       comparaison des modèles A et B, mémoire, concurrence
├── requirements.txt
└── README.md
```

## Modèle de clés

| Clé | Type | Contenu |
|---|---|---|
| `driver:{id}` | hash | `status`, `vehicle`, `lon`, `lat`, `ts`, `rider` ; TTL de 30 s renouvelé à chaque position |
| `geo:available` | GEO | chauffeurs libres, tous véhicules |
| `geo:available:{car,van,moto}` | GEO | chauffeurs libres d'un type de véhicule |
| `geo:all` | GEO | tous les chauffeurs actifs (modèle A, gardé pour la comparaison) |
| `drivers:lastseen` | sorted set | heure de la dernière position, sert au nettoyage des chauffeurs inactifs |
| `heat:{AAAAMMJJHH}` | hash | nombre de demandes par case de 0,01° ; TTL de 7 jours |

## Requêtes

| N° | Opération | Fonction |
|---|---|---|
| Q1 | Mise à jour de la position | `update_position` (Lua) |
| Q2 | Passage hors ligne | `set_offline` (Lua) |
| Q3 | Retrait des chauffeurs silencieux | `cleanup` (Lua) |
| Q4 | Chauffeurs libres dans un rayon | `search(radius_km=...)` |
| Q5 | N chauffeurs libres les plus proches | `search(count=N)` |
| Q6 | Recherche sans filtre de véhicule | `search(vehicle=None)` |
| Q7 | Recherche filtrée par véhicule | `search(vehicle="moto")` |
| Q8 | Réservation atomique | `reserve`, `reserve_nearest` (Lua) |
| Q9 | Fin de course | `release` (Lua) |
| Q10 | Fiche d'un chauffeur | `get_driver` |
| Q11 | Compteur de demandes par zone et par heure | `record_request` |
| Q12 | Heat map d'une heure | `heatmap` |
