"""
Projet 6 - Nearby-Driver Locator (Redis)
Etape 1 : nettoyage des donnees NYC Taxi et creation des chauffeurs simules.

Entree  : data/train.csv (Kaggle, NYC Taxi Trip Duration)
Sortie  : data/trips_clean.csv  (courses nettoyees, avec driver_id et vehicle)
          data/quality_report.txt (problemes de qualite trouves, pour le rapport)

Utilisation :
    python src/prepare_data.py
    python src/prepare_data.py --start 2016-03-07 --end 2016-03-14
"""

import argparse
import hashlib
import heapq
from pathlib import Path

import pandas as pd

# Zone de New York gardee (Manhattan, Brooklyn, Queens, aeroports JFK et LaGuardia)
LON_MIN, LON_MAX = -74.03, -73.75
LAT_MIN, LAT_MAX = 40.63, 40.85

# Durees de course acceptees (en secondes)
MIN_DURATION = 60          # moins d'une minute : erreur probable
MAX_DURATION = 3 * 3600    # plus de 3 heures : compteur oublie

# Repartition des types de vehicule (le jeu de donnees n'en a pas)
VEHICLES = [("car", 0.75), ("van", 0.15), ("moto", 0.10)]


def vehicle_for(driver_id: int) -> str:
    """Type de vehicule deterministe : le meme chauffeur a toujours le meme vehicule."""
    h = int(hashlib.md5(str(driver_id).encode()).hexdigest(), 16) % 100
    seuil = 0
    for name, part in VEHICLES:
        seuil += part * 100
        if h < seuil:
            return name
    return VEHICLES[-1][0]


def clean(df: pd.DataFrame, report: list) -> pd.DataFrame:
    report.append(f"Lignes brutes                         : {len(df):>9}")

    n = len(df)
    df = df.dropna()
    report.append(f"Lignes avec valeurs manquantes        : {n - len(df):>9}")

    n = len(df)
    df = df.drop_duplicates(subset="id")
    report.append(f"Doublons sur id                       : {n - len(df):>9}")

    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], errors="coerce")
    df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"], errors="coerce")
    n = len(df)
    df = df.dropna(subset=["pickup_datetime", "dropoff_datetime"])
    report.append(f"Dates illisibles                      : {n - len(df):>9}")

    n = len(df)
    in_box = (
        df["pickup_longitude"].between(LON_MIN, LON_MAX)
        & df["pickup_latitude"].between(LAT_MIN, LAT_MAX)
        & df["dropoff_longitude"].between(LON_MIN, LON_MAX)
        & df["dropoff_latitude"].between(LAT_MIN, LAT_MAX)
    )
    df = df[in_box]
    report.append(f"Coordonnees hors de New York          : {n - len(df):>9}")

    n = len(df)
    df = df[df["trip_duration"].between(MIN_DURATION, MAX_DURATION)]
    report.append(f"Durees < 1 min ou > 3 h               : {n - len(df):>9}")

    n = len(df)
    real = (df["dropoff_datetime"] - df["pickup_datetime"]).dt.total_seconds()
    df = df[(real - df["trip_duration"]).abs() <= 2]
    report.append(f"Duree incoherente avec les dates      : {n - len(df):>9}")

    n = len(df)
    df = df[df["passenger_count"] >= 1]
    report.append(f"Courses avec 0 passager               : {n - len(df):>9}")

    report.append(f"Lignes propres (toute la periode)     : {len(df):>9}")
    return df


def assign_drivers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Attribue un chauffeur a chaque course (le jeu de donnees n'a pas d'identifiant).
    Methode : on parcourt les courses par heure de depart. Le chauffeur libre depuis
    le plus longtemps prend la course. S'il n'y a aucun chauffeur libre, on en cree
    un nouveau. On obtient ainsi la plus petite flotte capable de faire toutes les
    courses, et chaque chauffeur a un trajet continu dans le temps.
    """
    df = df.sort_values("pickup_datetime").reset_index(drop=True)
    libres = []          # tas (heure ou le chauffeur est libre, driver_id)
    driver_ids = []
    prochain_id = 1

    for pickup, dropoff in zip(df["pickup_datetime"], df["dropoff_datetime"]):
        if libres and libres[0][0] <= pickup:
            _, d = heapq.heappop(libres)
        else:
            d = prochain_id
            prochain_id += 1
        driver_ids.append(d)
        heapq.heappush(libres, (dropoff, d))

    df["driver_id"] = driver_ids
    df["vehicle"] = df["driver_id"].map(vehicle_for)
    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/train.csv")
    parser.add_argument("--output", default="data/trips_clean.csv")
    parser.add_argument("--start", default="2016-03-07", help="debut inclus (AAAA-MM-JJ)")
    parser.add_argument("--end", default="2016-03-14", help="fin exclue (AAAA-MM-JJ)")
    args = parser.parse_args()

    report = []
    df = pd.read_csv(args.input)
    df = clean(df, report)

    # Sous-ensemble : une semaine complete (du lundi au dimanche)
    debut, fin = pd.Timestamp(args.start), pd.Timestamp(args.end)
    df = df[(df["pickup_datetime"] >= debut) & (df["pickup_datetime"] < fin)]
    report.append(f"Periode gardee                        : {args.start} -> {args.end}")
    report.append(f"Courses dans la periode               : {len(df):>9}")

    df = assign_drivers(df)
    report.append(f"Chauffeurs simules                    : {df['driver_id'].nunique():>9}")
    for v, c in df.drop_duplicates("driver_id")["vehicle"].value_counts().items():
        report.append(f"  dont {v:<5}                          : {c:>9}")

    out = pd.DataFrame({
        "trip_id": df["id"],
        "driver_id": df["driver_id"],
        "vehicle": df["vehicle"],
        "pickup_ts": (df["pickup_datetime"] - pd.Timestamp(0)) // pd.Timedelta("1s"),
        "dropoff_ts": (df["dropoff_datetime"] - pd.Timestamp(0)) // pd.Timedelta("1s"),
        "pickup_lon": df["pickup_longitude"].round(6),
        "pickup_lat": df["pickup_latitude"].round(6),
        "dropoff_lon": df["dropoff_longitude"].round(6),
        "dropoff_lat": df["dropoff_latitude"].round(6),
    })
    out.to_csv(args.output, index=False)

    texte = "\n".join(report)
    Path("data/quality_report.txt").write_text(texte + "\n")
    print(texte)
    print(f"\nFichier ecrit : {args.output}")


if __name__ == "__main__":
    main()
