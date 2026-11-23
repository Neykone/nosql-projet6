"""
Projet 6 - Nearby-Driver Locator (Redis)
Etape 4 : evaluation de performance.

Experiences :
  E1  latence de recherche selon la taille de la flotte (1 000, 10 000, 50 000 chauffeurs),
      modele A (un index + filtre) contre modele B (un index par groupe),
      sans filtre de vehicule (Q6) et avec filtre "moto" (Q7)
  E2  effet de la part de chauffeurs libres (heure creuse -> heure de pointe), flotte de 10 000
  E3  memoire occupee par les index de chaque modele
  E4  reservation concurrente : script Lua atomique contre version naive (lecture puis ecriture)

Les chauffeurs sont places sur de vrais points de depart de trips_clean.csv
(avec un petit decalage), pour garder la densite reelle de New York.

Sorties : results/benchmark.json et 3 graphiques PNG dans results/.

Utilisation :
    python src/benchmark.py
    python src/benchmark.py --queries 500     # plus rapide
"""

import argparse
import json
import os
import random
import threading
import time
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import redis

from redis_model import DriverStore, VEHICLES

SIZES = [1000, 10000, 50000]
RATIOS = [0.9, 0.5, 0.3, 0.1, 0.03]
RADIUS_KM = 2
COUNT = 5
SHARES = {"car": 0.75, "van": 0.15, "moto": 0.10}

COLOR_B = "#2a78d6"   # modele B
COLOR_A = "#eb6834"   # modele A
INK = "#3d3d3a"
MUTED = "#8a8a85"


def pct(values, q):
    s = sorted(values)
    return s[min(len(s) - 1, int(q * len(s)))] * 1000


def summary(values):
    return {"p50_ms": round(pct(values, 0.5), 3), "p95_ms": round(pct(values, 0.95), 3),
            "p99_ms": round(pct(values, 0.99), 3), "moyenne_ms": round(1000 * sum(values) / len(values), 3)}


def load_fleet(store, points, n, ratio, rng):
    """Remplit Redis avec n chauffeurs, dont une part `ratio` libres. Renvoie la duree de chargement."""
    store.wipe()
    now = int(time.time())
    vehicles = rng.choices(list(SHARES), weights=list(SHARES.values()), k=n)
    t0 = time.perf_counter()
    pipe = store.r.pipeline(transaction=False)
    for i in range(n):
        lon, lat = points[rng.randrange(len(points))]
        store.update_position(i, vehicles[i], lon + rng.uniform(-0.002, 0.002),
                              lat + rng.uniform(-0.002, 0.002), now=now, client=pipe)
        if i % 5000 == 4999:
            pipe.execute()
    pipe.execute()
    load_s = time.perf_counter() - t0
    # les autres chauffeurs passent en course (busy) via la reservation atomique
    busy = rng.sample(range(n), int(n * (1 - ratio)))
    for i in busy:
        keys = [store.k_driver(i), store.k_geo_av()] + store._all_vehicle_keys()
        store._reserve(keys=keys, args=[i, "bench"], client=pipe)
    pipe.execute()
    return load_s


def run_queries(store, queries, vehicle):
    lat_a, lat_b, trips_a, mismatch = [], [], [], 0
    for lon, lat in queries:
        t0 = time.perf_counter()
        rb = store.search(lon, lat, RADIUS_KM, count=COUNT, vehicle=vehicle)
        lat_b.append(time.perf_counter() - t0)

        t0 = time.perf_counter()
        ra, trips = store.search_model_a(lon, lat, RADIUS_KM, count=COUNT, vehicle=vehicle)
        lat_a.append(time.perf_counter() - t0)
        trips_a.append(trips)

        # controle : les deux modeles doivent trouver les memes distances
        if [d for _, d in ra] != [d for _, d in rb]:
            mismatch += 1
    return {"modele_B": summary(lat_b), "modele_A": summary(lat_a),
            "allers_retours_A_moyen": round(sum(trips_a) / len(trips_a), 2),
            "allers_retours_A_max": max(trips_a), "allers_retours_B": 1,
            "resultats_differents": mismatch}


def memory(store):
    mu = lambda k: store.r.memory_usage(k) or 0
    b = mu(store.k_geo_av()) + sum(mu(store.k_geo_av(v)) for v in VEHICLES)
    return {"index_A_geo_all_ko": round(mu(store.k_geo_all) / 1024, 1),
            "index_B_geo_available_ko": round(b / 1024, 1),
            "redis_total_mo": round(store.r.info("memory")["used_memory"] / 2**20, 2)}


def concurrency(store, threads=20, trials=100):
    """Plusieurs clients tentent de reserver le meme chauffeur au meme instant."""
    def one_trial(atomic):
        store.wipe()
        store.update_position("X", "car", -73.9855, 40.7580)
        winners = []
        barrier = threading.Barrier(threads)
        key = store.k_driver("X")

        def client(rid):
            barrier.wait()
            if atomic:
                if store.reserve("X", rid):
                    winners.append(rid)
            else:   # version naive : lecture, puis ecriture separee
                if store.r.hget(key, "status") == b"available":
                    store.r.hset(key, mapping={"status": "busy", "rider": rid})
                    winners.append(rid)

        ts = [threading.Thread(target=client, args=(f"r{i}",)) for i in range(threads)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        return len(winners)

    res = {}
    for name, atomic in (("lua_atomique", True), ("naive_hget_hset", False)):
        counts = [one_trial(atomic) for _ in range(trials)]
        res[name] = {"essais": trials, "clients_par_essai": threads,
                     "doubles_reservations": sum(1 for c in counts if c > 1),
                     "max_gagnants_sur_un_essai": max(counts)}
    return res


def style(ax, title, xlabel, ylabel):
    ax.set_title(title, loc="left", fontsize=12, color=INK, pad=12)
    ax.set_xlabel(xlabel, color=MUTED)
    ax.set_ylabel(ylabel, color=MUTED)
    ax.grid(axis="y", color="#e6e5e0", linewidth=0.8)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color("#c9c8c2")
    ax.tick_params(colors=MUTED)


def line(ax, x, y, color, label):
    ax.plot(x, y, color=color, linewidth=2, marker="o", markersize=7,
            markeredgecolor="white", markeredgewidth=2, label=label)
    ax.annotate(label, (x[-1], y[-1]), xytext=(8, 0), textcoords="offset points",
                va="center", color=INK, fontsize=9)


def charts(res, out):
    e1 = res["E1_taille_flotte"]
    x = [r["chauffeurs"] for r in e1]
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.2), sharey=True)
    for ax, key, title in ((axes[0], "sans_filtre", "Q6, tous vehicules"),
                           (axes[1], "filtre_moto", "Q7, motos seulement")):
        line(ax, x, [r[key]["modele_B"]["p95_ms"] for r in e1], COLOR_B, "Modele B")
        line(ax, x, [r[key]["modele_A"]["p95_ms"] for r in e1], COLOR_A, "Modele A")
        ax.set_xscale("log")
        ax.set_xticks(x, [f"{v:,}".replace(",", " ") for v in x])
        style(ax, title, "Chauffeurs dans Redis (echelle log)", "Latence p95 (ms)")
    axes[0].legend(frameon=False, loc="upper left")
    fig.suptitle("Latence de recherche selon la taille de la flotte (30 % de chauffeurs libres)",
                 x=0.01, ha="left", color=INK)
    fig.tight_layout()
    fig.savefig(out / "fig1_latence_taille_flotte.png", dpi=150)
    plt.close(fig)

    e2 = res["E2_part_libres"]
    x = [round(r["part_libres"] * 100) for r in e2]
    fig, ax = plt.subplots(figsize=(7, 4.2))
    line(ax, x, [r["sans_filtre"]["modele_B"]["p95_ms"] for r in e2], COLOR_B, "Modele B")
    line(ax, x, [r["sans_filtre"]["modele_A"]["p95_ms"] for r in e2], COLOR_A, "Modele A")
    ax.invert_xaxis()
    style(ax, "Latence p95 quand les chauffeurs libres se font rares (10 000 chauffeurs)",
          "Part de chauffeurs libres (%)", "Latence p95 (ms)")
    ax.legend(frameon=False, loc="upper left")
    fig.tight_layout()
    fig.savefig(out / "fig2_latence_part_libres.png", dpi=150)
    plt.close(fig)

    e3 = res["E3_memoire"]
    labels = [f"{r['chauffeurs']:,}".replace(",", " ") for r in e3]
    pos = range(len(e3))
    w = 0.36
    fig, ax = plt.subplots(figsize=(7, 4.2))
    ax.bar([p - w / 2 - 0.01 for p in pos], [r["index_A_geo_all_ko"] for r in e3], w,
           color=COLOR_A, label="Modele A : geo:all")
    ax.bar([p + w / 2 + 0.01 for p in pos], [r["index_B_geo_available_ko"] for r in e3], w,
           color=COLOR_B, label="Modele B : geo:available + 3 index vehicule")
    ax.set_xticks(list(pos), labels)
    style(ax, "Memoire des index geographiques (30 % de chauffeurs libres)",
          "Chauffeurs dans Redis", "Memoire (Ko)")
    ax.legend(frameon=False, loc="upper left")
    fig.tight_layout()
    fig.savefig(out / "fig3_memoire_index.png", dpi=150)
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data/trips_clean.csv")
    ap.add_argument("--queries", type=int, default=1000, help="recherches par mesure")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--port", type=int, default=int(os.environ.get("REDIS_PORT", 6379)))
    args = ap.parse_args()

    rng = random.Random(args.seed)
    df = pd.read_csv(args.input, usecols=["pickup_lon", "pickup_lat"])
    points = list(zip(df["pickup_lon"], df["pickup_lat"]))
    queries = [points[rng.randrange(len(points))] for _ in range(args.queries)]

    r = redis.Redis(port=args.port)
    store = DriverStore(r, prefix="bench:", ttl=3600)
    res = {"parametres": {**vars(args), "rayon_km": RADIUS_KM, "chauffeurs_demandes": COUNT},
           "E1_taille_flotte": [], "E2_part_libres": [], "E3_memoire": []}

    print("E1 et E3 : taille de la flotte")
    for n in SIZES:
        load_s = load_fleet(store, points, n, 0.3, rng)
        row = {"chauffeurs": n, "chargement_s": round(load_s, 2),
               "debit_chargement_par_s": round(n / load_s),
               "sans_filtre": run_queries(store, queries, None),
               "filtre_moto": run_queries(store, queries, "moto")}
        res["E1_taille_flotte"].append(row)
        res["E3_memoire"].append({"chauffeurs": n, **memory(store)})
        print(f"  {n:>6} chauffeurs  B p95={row['sans_filtre']['modele_B']['p95_ms']} ms  "
              f"A p95={row['sans_filtre']['modele_A']['p95_ms']} ms  "
              f"(moto : B {row['filtre_moto']['modele_B']['p95_ms']} / A {row['filtre_moto']['modele_A']['p95_ms']})")

    print("E2 : part de chauffeurs libres (10 000 chauffeurs)")
    for ratio in RATIOS:
        load_fleet(store, points, 10000, ratio, rng)
        row = {"part_libres": ratio, "sans_filtre": run_queries(store, queries, None)}
        res["E2_part_libres"].append(row)
        print(f"  {int(ratio * 100):>3} % libres  B p95={row['sans_filtre']['modele_B']['p95_ms']} ms  "
              f"A p95={row['sans_filtre']['modele_A']['p95_ms']} ms  "
              f"allers-retours A={row['sans_filtre']['allers_retours_A_moyen']}")

    print("E4 : reservation concurrente (20 clients, 100 essais)")
    res["E4_concurrence"] = concurrency(store)
    for k, v in res["E4_concurrence"].items():
        print(f"  {k:<16} doubles reservations : {v['doubles_reservations']} / {v['essais']}")

    store.wipe()
    out = Path("results")
    out.mkdir(exist_ok=True)
    (out / "benchmark.json").write_text(json.dumps(res, indent=2, ensure_ascii=False))
    charts(res, out)
    print("\nResultats : results/benchmark.json, results/fig1..fig3 *.png")


if __name__ == "__main__":
    main()
