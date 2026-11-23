"""
Projet 6 - Nearby-Driver Locator (Redis)
Etape 2 : modele de donnees Redis et toutes les requetes du projet.

Ce fichier est un module : le simulateur et le benchmark l'importent.
Lance seul, il execute une petite demonstration de chaque requete :
    python src/redis_model.py

Cles utilisees (toutes precedees d'un prefixe, "p6:" par defaut) :
    driver:{id}               hash  status, vehicle, lon, lat, ts, rider. TTL renouvele a chaque position
    geo:available             GEO   chauffeurs available, tous vehicules        (modele B)
    geo:available:{vehicle}   GEO   chauffeurs available d'un type de vehicule  (modele B)
    geo:all                   GEO   tous les chauffeurs actifs, quel que soit l'etat (modele A)
    drivers:lastseen          zset  score = heure de la derniere position (nettoyage des fantomes)
    heat:{AAAAMMJJHH}         hash  champ = zone, valeur = nombre de demandes. TTL 7 jours

Numerotation des requetes = liste des besoins du rapport (Q1 a Q12).
"""

import time
from datetime import datetime, timezone

import redis

VEHICLES = ("car", "van", "moto")
STATUSES = ("available", "busy", "offline")
SEARCH_MAX_KM = 50          # rayon utilise pour "les N plus proches" sans rayon impose
HEAT_TTL = 7 * 24 * 3600    # duree de vie d'une heure de heat map
GRID = 0.01                 # taille d'une case de heat map, en degres (environ 1 km)


# ---------------------------------------------------------------------------
# Scripts Lua. Redis execute un script en entier sans interruption :
# aucune autre commande ne peut s'intercaler, ce qui rend chaque operation atomique.
# Toutes les cles sont passees dans KEYS (obligatoire pour un futur Redis Cluster).
# ---------------------------------------------------------------------------

# Q1 : nouvelle position d'un chauffeur.
# KEYS : 1 driver hash, 2 lastseen, 3 geo:all, 4 geo:available, 5 geo:available:{vehicle}
# ARGV : 1 id, 2 vehicle, 3 lon, 4 lat, 5 now, 6 ttl
LUA_UPDATE = """
local status = redis.call('HGET', KEYS[1], 'status')
if not status then status = 'available' end
redis.call('HSET', KEYS[1], 'status', status, 'vehicle', ARGV[2],
           'lon', ARGV[3], 'lat', ARGV[4], 'ts', ARGV[5])
redis.call('EXPIRE', KEYS[1], ARGV[6])
redis.call('ZADD', KEYS[2], ARGV[5], ARGV[1])
if status == 'offline' then
  return status
end
redis.call('GEOADD', KEYS[3], ARGV[3], ARGV[4], ARGV[1])
if status == 'available' then
  redis.call('GEOADD', KEYS[4], ARGV[3], ARGV[4], ARGV[1])
  redis.call('GEOADD', KEYS[5], ARGV[3], ARGV[4], ARGV[1])
end
return status
"""

# Q8 : reservation atomique. Renvoie 1 si le chauffeur est reserve, 0 sinon.
# KEYS : 1 driver hash, 2 geo:available, 3..n geo:available:{vehicle} (un par vehicule)
# ARGV : 1 id, 2 rider
LUA_RESERVE = """
if redis.call('HGET', KEYS[1], 'status') ~= 'available' then
  return 0
end
redis.call('HSET', KEYS[1], 'status', 'busy', 'rider', ARGV[2])
for i = 2, #KEYS do
  redis.call('ZREM', KEYS[i], ARGV[1])
end
return 1
"""

# Q9 : fin de course, le chauffeur redevient available a sa nouvelle position.
# KEYS : 1 driver hash, 2 lastseen, 3 geo:all, 4 geo:available, 5 geo:available:{vehicle}
# ARGV : 1 id, 2 lon, 3 lat, 4 now, 5 ttl
LUA_RELEASE = """
if redis.call('HGET', KEYS[1], 'status') ~= 'busy' then
  return 0
end
local vehicle = redis.call('HGET', KEYS[1], 'vehicle')
redis.call('HSET', KEYS[1], 'status', 'available', 'lon', ARGV[2], 'lat', ARGV[3], 'ts', ARGV[4])
redis.call('HDEL', KEYS[1], 'rider')
redis.call('EXPIRE', KEYS[1], ARGV[5])
redis.call('ZADD', KEYS[2], ARGV[4], ARGV[1])
redis.call('GEOADD', KEYS[3], ARGV[2], ARGV[3], ARGV[1])
redis.call('GEOADD', KEYS[4], ARGV[2], ARGV[3], ARGV[1])
redis.call('GEOADD', KEYS[5], ARGV[2], ARGV[3], ARGV[1])
return 1
"""

# Q2 : passage en offline (fin de service). Le chauffeur quitte tous les index.
# KEYS : 1 driver hash, 2 lastseen, 3 geo:all, 4 geo:available, 5..n geo:available:{vehicle}
# ARGV : 1 id
LUA_OFFLINE = """
if redis.call('EXISTS', KEYS[1]) == 0 then
  return 0
end
redis.call('HSET', KEYS[1], 'status', 'offline')
redis.call('ZREM', KEYS[2], ARGV[1])
for i = 3, #KEYS do
  redis.call('ZREM', KEYS[i], ARGV[1])
end
return 1
"""

# Q3 : nettoyage des fantomes (chauffeurs silencieux depuis plus de max_age secondes).
# KEYS : 1 lastseen, 2 geo:all, 3 geo:available, 4..n geo:available:{vehicle}
# ARGV : 1 limite (now - max_age), 2 taille maximale du lot
LUA_CLEANUP = """
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
if #ids == 0 then
  return 0
end
for i = 1, #KEYS do
  redis.call('ZREM', KEYS[i], unpack(ids))
end
return #ids
"""


class DriverStore:
    def __init__(self, r: redis.Redis, prefix: str = "p6:", ttl: int = 30):
        self.r = r
        self.p = prefix
        self.ttl = ttl  # secondes sans position avant qu'un chauffeur soit considere perdu
        self._update = r.register_script(LUA_UPDATE)
        self._reserve = r.register_script(LUA_RESERVE)
        self._release = r.register_script(LUA_RELEASE)
        self._offline = r.register_script(LUA_OFFLINE)
        self._cleanup = r.register_script(LUA_CLEANUP)

    # ---- noms de cles ----
    def k_driver(self, d):
        return f"{self.p}driver:{d}"

    def k_geo_av(self, vehicle=None):
        return f"{self.p}geo:available" + (f":{vehicle}" if vehicle else "")

    @property
    def k_geo_all(self):
        return f"{self.p}geo:all"

    @property
    def k_seen(self):
        return f"{self.p}drivers:lastseen"

    def k_heat(self, ts):
        return f"{self.p}heat:" + datetime.fromtimestamp(ts, timezone.utc).strftime("%Y%m%d%H")

    def _all_vehicle_keys(self):
        return [self.k_geo_av(v) for v in VEHICLES]

    # ---- Q1 : mise a jour de position ----
    def update_position(self, driver_id, vehicle, lon, lat, now=None, client=None):
        now = int(now if now is not None else time.time())
        keys = [self.k_driver(driver_id), self.k_seen, self.k_geo_all,
                self.k_geo_av(), self.k_geo_av(vehicle)]
        return self._update(keys=keys, args=[driver_id, vehicle, lon, lat, now, self.ttl],
                            client=client)

    # ---- Q2 : passage offline ----
    def set_offline(self, driver_id):
        keys = [self.k_driver(driver_id), self.k_seen, self.k_geo_all,
                self.k_geo_av()] + self._all_vehicle_keys()
        return self._offline(keys=keys, args=[driver_id])

    # ---- Q3 : nettoyage des fantomes ----
    def cleanup(self, now=None, max_age=None, batch=1000):
        now = int(now if now is not None else time.time())
        max_age = max_age if max_age is not None else self.ttl
        keys = [self.k_seen, self.k_geo_all, self.k_geo_av()] + self._all_vehicle_keys()
        total = 0
        while True:
            n = self._cleanup(keys=keys, args=[now - max_age, batch])
            total += n
            if n < batch:
                return total

    # ---- Q4 a Q7 : recherches, modele B (un index par groupe) ----
    def search(self, lon, lat, radius_km=None, count=None, vehicle=None):
        """Chauffeurs available autour d'un point, du plus proche au plus loin.
        radius_km=None : les `count` plus proches sans limite de rayon (Q5).
        vehicle=None   : tous types de vehicule (Q6), sinon filtre (Q7)."""
        res = self.r.geosearch(
            self.k_geo_av(vehicle), longitude=lon, latitude=lat,
            radius=radius_km or SEARCH_MAX_KM, unit="km",
            sort="ASC", count=count, withdist=True)
        return [(m.decode() if isinstance(m, bytes) else m, round(dist, 3)) for m, dist in res]

    # ---- Q4 a Q7 : recherches, modele A (un seul index + filtre) ----
    def search_model_a(self, lon, lat, radius_km=None, count=5, vehicle=None):
        """Meme resultat que search(), mais avec un seul index geo:all.
        On lit l'etat de chaque candidat et on jette ceux qui ne conviennent pas.
        Si on n'en trouve pas assez, on redemande plus de candidats.
        Renvoie (resultats, nombre d'allers-retours vers Redis)."""
        ask = count * 4
        trips = 0
        while True:
            cands = self.r.geosearch(
                self.k_geo_all, longitude=lon, latitude=lat,
                radius=radius_km or SEARCH_MAX_KM, unit="km",
                sort="ASC", count=ask, withdist=True)
            trips += 1
            pipe = self.r.pipeline(transaction=False)
            for m, _ in cands:
                pipe.hmget(f"{self.p}driver:{m.decode() if isinstance(m, bytes) else m}",
                           "status", "vehicle")
            infos = pipe.execute()
            trips += 1
            out = []
            for (m, dist), (st, veh) in zip(cands, infos):
                if st != b"available" and st != "available":
                    continue
                veh = veh.decode() if isinstance(veh, bytes) else veh
                if vehicle and veh != vehicle:
                    continue
                out.append((m.decode() if isinstance(m, bytes) else m, round(dist, 3)))
                if len(out) == count:
                    return out, trips
            if len(cands) < ask:      # plus aucun candidat dans le rayon
                return out, trips
            ask *= 2

    # ---- Q8 : reservation atomique ----
    def reserve(self, driver_id, rider_id):
        keys = [self.k_driver(driver_id), self.k_geo_av()] + self._all_vehicle_keys()
        return bool(self._reserve(keys=keys, args=[driver_id, rider_id]))

    def reserve_nearest(self, rider_id, lon, lat, radius_km=3, vehicle=None, tries=5):
        """Cherche les chauffeurs proches et reserve le premier qui est encore libre.
        Si un autre client l'a pris entre la recherche et la reservation, on passe au suivant."""
        for driver_id, dist in self.search(lon, lat, radius_km, count=tries, vehicle=vehicle):
            if self.reserve(driver_id, rider_id):
                return driver_id, dist
        return None, None

    # ---- Q9 : liberation en fin de course ----
    def release(self, driver_id, vehicle, lon, lat, now=None):
        now = int(now if now is not None else time.time())
        keys = [self.k_driver(driver_id), self.k_seen, self.k_geo_all,
                self.k_geo_av(), self.k_geo_av(vehicle)]
        return bool(self._release(keys=keys, args=[driver_id, lon, lat, now, self.ttl]))

    # ---- Q10 : fiche d'un chauffeur ----
    def get_driver(self, driver_id):
        h = self.r.hgetall(self.k_driver(driver_id))
        if not h:
            return None
        d = {k.decode() if isinstance(k, bytes) else k: v.decode() if isinstance(v, bytes) else v
             for k, v in h.items()}
        d["ttl_restant"] = self.r.ttl(self.k_driver(driver_id))
        return d

    # ---- Q11 : compteur de demandes par zone et par heure ----
    @staticmethod
    def zone_of(lon, lat):
        """Case de grille de 0,01 degre contenant le point, ex. '40.75:-73.99'."""
        return f"{int(lat // GRID) * GRID:.2f}:{int(lon // GRID) * GRID:.2f}"

    def record_request(self, lon, lat, ts=None, client=None):
        ts = int(ts if ts is not None else time.time())
        key = self.k_heat(ts)
        c = client or self.r.pipeline(transaction=False)
        c.hincrby(key, self.zone_of(lon, lat), 1)
        c.expire(key, HEAT_TTL, nx=True)   # pose le TTL seulement a la creation de la cle
        if client is None:
            c.execute()

    # ---- Q12 : lecture de la heat map d'une heure ----
    def heatmap(self, ts, top=10):
        h = self.r.hgetall(self.k_heat(ts))
        rows = [((z.decode() if isinstance(z, bytes) else z), int(n)) for z, n in h.items()]
        rows.sort(key=lambda x: -x[1])
        return rows[:top] if top else rows

    # ---- utilitaires ----
    def counts(self):
        return {
            "geo:all": self.r.zcard(self.k_geo_all),
            "geo:available": self.r.zcard(self.k_geo_av()),
            **{f"geo:available:{v}": self.r.zcard(self.k_geo_av(v)) for v in VEHICLES},
            "lastseen": self.r.zcard(self.k_seen),
        }

    def wipe(self):
        """Supprime toutes les cles du projet (et seulement elles)."""
        n = 0
        for key in self.r.scan_iter(match=f"{self.p}*", count=1000):
            n += self.r.delete(key)
        return n


def demo():
    r = redis.Redis(host="localhost", port=int(__import__("os").environ.get("REDIS_PORT", 6379)))
    s = DriverStore(r, prefix="demo:", ttl=30)
    s.wipe()
    now = int(time.time())

    print("Q1  positions de 4 chauffeurs autour de Times Square")
    s.update_position(1, "car", -73.9855, 40.7580, now)
    s.update_position(2, "car", -73.9772, 40.7527, now)
    s.update_position(3, "van", -73.9900, 40.7500, now)
    s.update_position(4, "moto", -73.9700, 40.7650, now - 60)   # silencieux depuis 1 minute
    print("    ", s.counts())

    print("Q3  nettoyage des fantomes :", s.cleanup(now), "chauffeur(s) retire(s) des index")
    print("    ", s.counts())

    print("Q4  rayon 1 km               :", s.search(-73.9855, 40.7580, radius_km=1))
    print("Q5  2 plus proches            :", s.search(-73.9855, 40.7580, count=2))
    print("Q7  vans seulement            :", s.search(-73.9855, 40.7580, vehicle="van"))
    print("    modele A (meme requete)   :", s.search_model_a(-73.9855, 40.7580, count=2))

    print("Q8  client A reserve le chauffeur 1 :", s.reserve(1, "riderA"))
    print("    client B reserve le chauffeur 1 :", s.reserve(1, "riderB"))
    print("    reserve_nearest pour client C  :", s.reserve_nearest("riderC", -73.9855, 40.7580))
    print("Q10 fiche du chauffeur 1    :", s.get_driver(1))
    print("Q1  position pendant la course (reste busy) :",
          s.update_position(1, "car", -73.9800, 40.7600, now))
    print("    recherche, 1 n'apparait plus :", s.search(-73.9855, 40.7580))

    print("Q9  fin de course du chauffeur 1 :", s.release(1, "car", -73.9600, 40.7700, now))
    print("    recherche, 1 est revenu       :", s.search(-73.9855, 40.7580))

    print("Q2  chauffeur 3 offline :", s.set_offline(3), s.counts())

    for lon, lat in [(-73.9855, 40.7580)] * 3 + [(-73.9772, 40.7527)]:
        s.record_request(lon, lat, now)
    print("Q11/Q12 heat map de l'heure courante :", s.heatmap(now))

    s.wipe()
    print("\nDemo terminee, cles demo:* supprimees.")


if __name__ == "__main__":
    demo()
