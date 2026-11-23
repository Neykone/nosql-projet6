"""
Projet 6 - Nearby-Driver Locator (Redis)
Etape 3 : simulateur. Rejoue les courses de trips_clean.csv comme des chauffeurs qui roulent.

Ce que fait le simulateur, seconde par seconde (temps simule) :
  - chaque chauffeur en service envoie sa position toutes les PING secondes (Q1) ;
    pendant une course il avance du depart vers l'arrivee, entre deux courses
    il roule vers le depart de sa course suivante ;
  - a l'heure de depart d'une course, un client fait une demande :
    compteur de heat map (Q11), recherche des chauffeurs proches (Q6),
    puis reservation atomique du chauffeur de la course (Q8) ;
  - a l'heure d'arrivee, le chauffeur est libere (Q9) ;
  - un chauffeur sans course pendant plus d'une heure arrete d'envoyer sa position :
    le nettoyage (Q3), lance toutes les 5 secondes, le retire des index.

Option --fleet : multiplie la flotte (copies des chauffeurs decalees de quelques
centaines de metres) pour mesurer la latence quand le nombre de chauffeurs augmente.

Exemples :
    python src/simulate.py                          # lundi 7 mars 2016, flotte x1, vitesse maximale
    python src/simulate.py --fleet 10 --hours 6     # 6 heures, flotte x10
    python src/simulate.py --speed 60               # 1 minute simulee = 1 seconde reelle
"""

import argparse
import heapq
import json
import os
import random
import time
from pathlib import Path

import pandas as pd
import redis

from redis_model import DriverStore

PING = 15            # secondes simulees entre deux positions d'un chauffeur
STALE = 30           # un chauffeur silencieux depuis 30 s simulees est un fantome
CLEANUP_EVERY = 5    # periode du nettoyage des fantomes, en secondes simulees
MAX_IDLE = 3600      # au-dela d'une heure sans course, le chauffeur se deconnecte
LEAD = 600           # il se reconnecte 10 min avant sa course suivante
LINGER = 600         # et reste connecte 10 min apres sa derniere course
SEARCH_KM = 2        # rayon de recherche d'un client
SEARCH_N = 5         # nombre de chauffeurs proposes au client


class Driver:
    __slots__ = ("id", "vehicle", "trips", "k", "dlon", "dlat", "copy")

    def __init__(self, did, vehicle, trips, dlon=0.0, dlat=0.0, copy=False):
        self.id = did
        self.vehicle = vehicle
        self.trips = trips   # liste de (pickup_ts, dropoff_ts, plon, plat, dlon, dlat, trip_id)
        self.k = 0           # indice de la premiere course pas encore terminee
        self.dlon, self.dlat = dlon, dlat
        self.copy = copy

    def position(self, t):
        """Position du chauffeur a l'instant t, ou None s'il est deconnecte."""
        tr = self.trips
        while self.k < len(tr) and tr[self.k][1] <= t:
            self.k += 1
        prev = tr[self.k - 1] if self.k > 0 else None

        if self.k == len(tr):                         # apres sa derniere course
            if prev and t < prev[1] + LINGER:
                return self._at(prev[4], prev[5])
            return None

        cur = tr[self.k]
        if t >= cur[0]:                               # en course : depart -> arrivee
            return self._between(cur[2], cur[3], cur[4], cur[5], (t - cur[0]) / (cur[1] - cur[0]))
        if prev is None:                              # avant sa premiere course
            return self._at(cur[2], cur[3]) if t >= cur[0] - LEAD else None
        if cur[0] - prev[1] <= MAX_IDLE:              # roule vers la course suivante
            return self._between(prev[4], prev[5], cur[2], cur[3],
                                 (t - prev[1]) / max(1, cur[0] - prev[1]))
        if t < prev[1] + LINGER:                      # longue pause : reste un peu, puis se tait
            return self._at(prev[4], prev[5])
        if t >= cur[0] - LEAD:
            return self._at(cur[2], cur[3])
        return None

    def _at(self, lon, lat):
        return lon + self.dlon, lat + self.dlat

    def _between(self, lon1, lat1, lon2, lat2, f):
        return self._at(lon1 + (lon2 - lon1) * f, lat1 + (lat2 - lat1) * f)


def load_drivers(path, start, end, fleet, seed):
    df = pd.read_csv(path)
    df = df[(df["pickup_ts"] >= start) & (df["pickup_ts"] < end)].sort_values("pickup_ts")
    rng = random.Random(seed)
    drivers = []
    for did, g in df.groupby("driver_id"):
        trips = list(zip(g["pickup_ts"], g["dropoff_ts"], g["pickup_lon"], g["pickup_lat"],
                         g["dropoff_lon"], g["dropoff_lat"], g["trip_id"]))
        vehicle = g["vehicle"].iloc[0]
        drivers.append(Driver(str(did), vehicle, trips))
        for c in range(1, fleet):    # copies decalees d'au plus ~300 m
            drivers.append(Driver(f"{did}c{c}", vehicle, trips,
                                  rng.uniform(-0.004, 0.004), rng.uniform(-0.003, 0.003), True))
    return drivers, len(df)


def percentiles(values):
    if not values:
        return {}
    s = sorted(values)
    pick = lambda q: round(s[min(len(s) - 1, int(q * len(s)))] * 1000, 3)
    return {"p50_ms": pick(0.50), "p95_ms": pick(0.95), "p99_ms": pick(0.99),
            "max_ms": round(s[-1] * 1000, 3), "n": len(s)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data/trips_clean.csv")
    ap.add_argument("--day", default="2016-03-07", help="premier jour rejoue (AAAA-MM-JJ)")
    ap.add_argument("--hours", type=int, default=24, help="duree rejouee, en heures")
    ap.add_argument("--fleet", type=int, default=1, help="multiplicateur de flotte")
    ap.add_argument("--speed", type=float, default=0,
                    help="secondes simulees par seconde reelle (0 = le plus vite possible)")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=int(os.environ.get("REDIS_PORT", 6379)))
    args = ap.parse_args()

    start = int(pd.Timestamp(args.day).timestamp())
    end = start + args.hours * 3600

    r = redis.Redis(host=args.host, port=args.port)
    store = DriverStore(r, prefix="p6:", ttl=STALE)
    print(f"Suppression des anciennes cles p6:* : {store.wipe()}")
    r.config_resetstat()

    drivers, n_trips = load_drivers(args.input, start, end, args.fleet, args.seed)
    print(f"{n_trips} courses, {len(drivers)} chauffeurs (flotte x{args.fleet}), "
          f"de {args.day} pendant {args.hours} h")

    # Evenements de course : (instant, ordre, type, chauffeur, course). Les fins passent avant les debuts.
    events = []
    for d in drivers:
        for tr in d.trips:
            events.append((tr[1], 0, "end", d, tr))
            events.append((tr[0], 1, "start", d, tr))
    events.sort(key=lambda e: (e[0], e[1]))
    ev_i = 0

    # File des prochaines positions : (instant, rang, chauffeur). Pings decales pour eviter les pics.
    pings = [(start + i % PING, i, d) for i, d in enumerate(drivers)]
    heapq.heapify(pings)

    stats = {"pings": 0, "requests": 0, "requests_with_driver": 0, "reserve_ok": 0,
             "reserve_fail": 0, "releases": 0, "ghosts_removed": 0}
    lat_search, lat_reserve, lat_ping_batch = [], [], []
    online_samples = []
    t_real0 = time.perf_counter()

    for t in range(start, end):
        # 1. fins puis debuts de course de cette seconde
        while ev_i < len(events) and events[ev_i][0] <= t:
            ts, _, kind, d, tr = events[ev_i]
            ev_i += 1
            if kind == "end":
                if store.release(d.id, d.vehicle, tr[4] + d.dlon, tr[5] + d.dlat, now=ts):
                    stats["releases"] += 1
                continue
            if not d.copy:                          # un vrai client fait une demande
                lon, lat = tr[2], tr[3]
                store.record_request(lon, lat, ts)
                t0 = time.perf_counter()
                found = store.search(lon, lat, SEARCH_KM, count=SEARCH_N)
                lat_search.append(time.perf_counter() - t0)
                stats["requests"] += 1
                stats["requests_with_driver"] += bool(found)
            t0 = time.perf_counter()
            ok = store.reserve(d.id, tr[6] if not d.copy else "copy")
            lat_reserve.append(time.perf_counter() - t0)
            stats["reserve_ok" if ok else "reserve_fail"] += 1

        # 2. positions dues a cette seconde, envoyees en un seul pipeline
        pipe = r.pipeline(transaction=False)
        n = 0
        while pings and pings[0][0] <= t:
            _, rank, d = heapq.heappop(pings)
            pos = d.position(t)
            if pos is not None:
                store.update_position(d.id, d.vehicle, pos[0], pos[1], now=t, client=pipe)
                n += 1
            heapq.heappush(pings, (t + PING, rank, d))
        if n:
            t0 = time.perf_counter()
            pipe.execute()
            lat_ping_batch.append((time.perf_counter() - t0) / n)
            stats["pings"] += n

        # 3. nettoyage des fantomes
        if t % CLEANUP_EVERY == 0:
            stats["ghosts_removed"] += store.cleanup(now=t, max_age=STALE)

        # 4. une mesure du nombre de chauffeurs en ligne par heure simulee
        if (t - start) % 3600 == 0:
            c = store.counts()
            online_samples.append({"heure": (t - start) // 3600,
                                   "actifs": c["geo:all"], "available": c["geo:available"]})
            print(f"  h{(t - start) // 3600:02d}  actifs={c['geo:all']:>6}  "
                  f"available={c['geo:available']:>6}  pings={stats['pings']:>9}")

        # 5. vitesse de rejeu
        if args.speed > 0:
            late = (t - start) / args.speed - (time.perf_counter() - t_real0)
            if late > 0:
                time.sleep(late)

    real = time.perf_counter() - t_real0
    mem = r.info("memory")
    result = {
        "parametres": vars(args),
        "chauffeurs": len(drivers),
        "courses": n_trips,
        "duree_reelle_s": round(real, 1),
        "debit_ecritures_par_s": round(stats["pings"] / real, 1),
        **stats,
        "latence_recherche_Q6": percentiles(lat_search),
        "latence_reservation_Q8": percentiles(lat_reserve),
        "latence_moyenne_par_position_Q1_ms": round(1000 * sum(lat_ping_batch) / max(1, len(lat_ping_batch)), 4),
        "memoire_redis_mo": round(mem["used_memory"] / 2**20, 2),
        "chauffeurs_en_ligne_par_heure": online_samples,
        "heat_map_top5_heure_18": store.heatmap(start + 18 * 3600, top=5) if args.hours > 18 else None,
    }

    Path("results").mkdir(exist_ok=True)
    out = Path(f"results/simulation_fleet{args.fleet}_{args.hours}h.json")
    out.write_text(json.dumps(result, indent=2, ensure_ascii=False))

    print("\n=== Resultats ===")
    for k in ("chauffeurs", "courses", "duree_reelle_s", "pings", "debit_ecritures_par_s",
              "requests", "requests_with_driver", "reserve_ok", "reserve_fail",
              "releases", "ghosts_removed", "memoire_redis_mo"):
        print(f"{k:<26}: {result[k]}")
    print(f"{'latence recherche Q6':<26}: {result['latence_recherche_Q6']}")
    print(f"{'latence reservation Q8':<26}: {result['latence_reservation_Q8']}")
    print(f"{'latence par position Q1':<26}: {result['latence_moyenne_par_position_Q1_ms']} ms")
    print(f"\nResultats complets : {out}")


if __name__ == "__main__":
    main()
