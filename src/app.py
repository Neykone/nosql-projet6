"""
Projet 6 - Nearby-Driver Locator (Redis)
Interface web de demonstration : carte des chauffeurs en direct, recherche, reservation, heat map.

Utilisation (deux terminaux Ubuntu, dans ~/projet6 avec le venv active) :
    terminal 1 : python src/simulate.py --speed 60     # 1 minute simulee par seconde reelle
    terminal 2 : python src/app.py
puis ouvrir http://localhost:5000 dans le navigateur Windows.

L'interface lit les cles p6:* ecrites par le simulateur et utilise les fonctions de redis_model.py.
"""

import os
import time
import uuid
from datetime import datetime, timezone

import redis
from flask import Flask, jsonify, request

from redis_model import DriverStore, GRID

r = redis.Redis(host="localhost", port=int(os.environ.get("REDIS_PORT", 6379)))
store = DriverStore(r, prefix="p6:", ttl=30)
app = Flask(__name__)


def txt(v):
    return v.decode() if isinstance(v, bytes) else v


def sim_now():
    """Heure simulee courante = la position la plus recente recue."""
    last = r.zrevrange(store.k_seen, 0, 0, withscores=True)
    return int(last[0][1]) if last else int(time.time())


@app.get("/api/drivers")
def drivers():
    members = r.zrange(store.k_geo_all, 0, -1)
    pipe = r.pipeline(transaction=False)
    for m in members:
        pipe.hmget(store.k_driver(txt(m)), "status", "vehicle", "lon", "lat")
    out = []
    for m, (st, veh, lon, lat) in zip(members, pipe.execute()):
        if lon is None:
            continue
        out.append({"id": txt(m), "status": txt(st), "vehicle": txt(veh),
                    "lon": float(lon), "lat": float(lat)})
    now = sim_now()
    return jsonify({
        "drivers": out,
        "counts": store.counts(),
        "sim_time": datetime.fromtimestamp(now, timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    })


def with_coords(results):
    pipe = r.pipeline(transaction=False)
    for d, _ in results:
        pipe.hmget(store.k_driver(d), "lon", "lat", "vehicle")
    rows = []
    for (d, dist), (lon, lat, veh) in zip(results, pipe.execute()):
        if lon is not None:
            rows.append({"id": d, "dist_km": dist, "lon": float(lon), "lat": float(lat),
                         "vehicle": txt(veh)})
    return rows


@app.get("/api/search")
def search():
    lon, lat = float(request.args["lon"]), float(request.args["lat"])
    radius = float(request.args.get("radius", 2))
    count = int(request.args.get("count", 5))
    vehicle = request.args.get("vehicle") or None
    model = request.args.get("model", "B")
    t0 = time.perf_counter()
    if model == "A":
        res, trips = store.search_model_a(lon, lat, radius, count=count, vehicle=vehicle)
    else:
        res, trips = store.search(lon, lat, radius, count=count, vehicle=vehicle), 1
    ms = (time.perf_counter() - t0) * 1000
    store.record_request(lon, lat, sim_now())
    return jsonify({"results": with_coords(res), "latency_ms": round(ms, 3),
                    "round_trips": trips, "model": model})


@app.post("/api/reserve")
def reserve():
    body = request.get_json()
    rider = "ui-" + uuid.uuid4().hex[:6]
    t0 = time.perf_counter()
    d, dist = store.reserve_nearest(rider, body["lon"], body["lat"],
                                    float(body.get("radius", 2)), body.get("vehicle") or None)
    ms = (time.perf_counter() - t0) * 1000
    return jsonify({"driver": d, "dist_km": dist, "rider": rider, "latency_ms": round(ms, 3),
                    "fiche": store.get_driver(d) if d else None})


@app.post("/api/release")
def release():
    d = str(request.get_json()["id"])
    info = store.get_driver(d)
    if not info:
        return jsonify({"ok": False, "raison": "chauffeur inconnu ou expire"})
    ok = store.release(d, info["vehicle"], float(info["lon"]), float(info["lat"]), now=sim_now())
    return jsonify({"ok": ok, "fiche": store.get_driver(d)})


@app.get("/api/driver/<d>")
def driver(d):
    return jsonify(store.get_driver(d))


@app.get("/api/heatmap")
def heatmap():
    now = sim_now()
    rows = store.heatmap(now, top=0)
    zones = []
    for z, n in rows:
        lat, lon = (float(x) for x in z.split(":"))
        zones.append({"zone": z, "lat": lat, "lon": lon, "n": n})
    hour = datetime.fromtimestamp(now, timezone.utc).strftime("%Y-%m-%d %Hh")
    return jsonify({"heure": hour, "grid": GRID, "zones": zones})


@app.get("/")
def index():
    return PAGE


PAGE = """<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="utf-8">
<title>Projet 6 - Nearby-Driver Locator</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
  :root { --ink:#2b2b28; --muted:#77766f; --line:#e3e2dc; --bg:#f7f6f2;
          --free:#2a78d6; --busy:#eb6834; --rider:#1b1b19; }
  * { box-sizing:border-box; }
  body { margin:0; font:14px/1.45 system-ui, sans-serif; color:var(--ink); background:var(--bg);
         display:grid; grid-template-columns:340px 1fr; height:100vh; }
  aside { padding:18px; overflow-y:auto; border-right:1px solid var(--line); background:#fff; }
  h1 { font-size:17px; margin:0 0 2px; }
  .sub { color:var(--muted); margin:0 0 16px; font-size:12px; }
  h2 { font-size:12px; text-transform:uppercase; letter-spacing:.05em; color:var(--muted);
       margin:18px 0 8px; }
  .stats { display:grid; grid-template-columns:1fr 1fr; gap:8px; }
  .stat { border:1px solid var(--line); border-radius:8px; padding:8px 10px; }
  .stat b { display:block; font-size:20px; font-variant-numeric:tabular-nums; }
  .stat span { color:var(--muted); font-size:12px; }
  label { display:flex; justify-content:space-between; align-items:center; margin:6px 0; }
  select, input { font:inherit; padding:3px 6px; border:1px solid var(--line); border-radius:6px; width:130px; }
  button { font:inherit; padding:7px 10px; border-radius:6px; border:1px solid var(--ink);
           background:var(--ink); color:#fff; cursor:pointer; width:100%; margin-top:6px; }
  button.ghost { background:#fff; color:var(--ink); }
  button:disabled { opacity:.4; cursor:default; }
  .legend { display:flex; gap:14px; font-size:12px; color:var(--muted); }
  .dot { display:inline-block; width:10px; height:10px; border-radius:50%; margin-right:5px;
         vertical-align:-1px; border:2px solid #fff; box-shadow:0 0 0 1px #ccc; }
  table { width:100%; border-collapse:collapse; font-size:13px; font-variant-numeric:tabular-nums; }
  td, th { padding:4px 2px; border-bottom:1px solid var(--line); text-align:left; }
  th { color:var(--muted); font-weight:500; font-size:12px; }
  #msg { font-size:13px; padding:8px 10px; border-radius:6px; background:var(--bg); min-height:20px; }
  #map { height:100vh; }
  .hint { color:var(--muted); font-size:12px; }
</style>
</head>
<body>
<aside>
  <h1>Nearby-Driver Locator</h1>
  <p class="sub">Projet 6 NoSQL, Redis. Heure simulee : <b id="clock">-</b></p>

  <div class="stats">
    <div class="stat"><b id="c-all">-</b><span>chauffeurs actifs</span></div>
    <div class="stat"><b id="c-av">-</b><span>chauffeurs libres</span></div>
  </div>
  <p class="legend" style="margin-top:10px">
    <span><i class="dot" style="background:var(--free)"></i>libre</span>
    <span><i class="dot" style="background:var(--busy)"></i>en course</span>
    <span><i class="dot" style="background:var(--rider)"></i>client</span>
  </p>

  <h2>Recherche (cliquer sur la carte)</h2>
  <label>Rayon (km) <input id="radius" type="number" value="2" min="0.2" step="0.5"></label>
  <label>Nombre <input id="count" type="number" value="5" min="1" max="20"></label>
  <label>Vehicule <select id="vehicle"><option value="">tous</option>
    <option>car</option><option>van</option><option>moto</option></select></label>
  <label>Modele <select id="model"><option value="B">B : index par groupe</option>
    <option value="A">A : index unique + filtre</option></select></label>
  <p class="hint">Clique sur la carte pour placer le client.</p>

  <div id="msg">Aucune recherche.</div>
  <table id="res"></table>
  <button id="btn-reserve" disabled>Reserver le plus proche (Q8)</button>
  <button id="btn-release" class="ghost" disabled>Liberer le chauffeur reserve (Q9)</button>

  <h2>Heat map de l'heure (Q12)</h2>
  <label>Afficher sur la carte <input id="show-heat" type="checkbox" style="width:auto"></label>
  <p class="hint" id="heat-hour"></p>
  <table id="heat"></table>
</aside>
<div id="map"></div>

<script>
const map = L.map('map').setView([40.75, -73.98], 13);
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
  {maxZoom: 19, attribution: '&copy; OpenStreetMap'}).addTo(map);

const driverLayer = L.layerGroup().addTo(map);
const searchLayer = L.layerGroup().addTo(map);
const heatLayer = L.layerGroup().addTo(map);
let rider = null, reserved = null;
const $ = id => document.getElementById(id);
const css = n => getComputedStyle(document.documentElement).getPropertyValue(n).trim();

async function refresh() {
  const d = await (await fetch('/api/drivers')).json();
  $('clock').textContent = d.sim_time;
  $('c-all').textContent = d.counts['geo:all'];
  $('c-av').textContent = d.counts['geo:available'];
  driverLayer.clearLayers();
  for (const x of d.drivers) {
    const free = x.status === 'available';
    L.circleMarker([x.lat, x.lon], {radius: 5, weight: 2, color: '#fff',
      fillColor: free ? css('--free') : css('--busy'), fillOpacity: 1})
      .bindTooltip(`Chauffeur ${x.id}<br>${x.vehicle}, ${free ? 'libre' : 'en course'}`)
      .addTo(driverLayer);
  }
}

function params() {
  return {radius: $('radius').value, count: $('count').value,
          vehicle: $('vehicle').value, model: $('model').value};
}

async function search() {
  if (!rider) return;
  const p = params();
  const q = new URLSearchParams({lon: rider.lng, lat: rider.lat, ...p});
  const d = await (await fetch('/api/search?' + q)).json();
  searchLayer.clearLayers();
  L.circle(rider, {radius: p.radius * 1000, color: css('--rider'), weight: 1,
                   fillOpacity: 0.04, dashArray: '4 4'}).addTo(searchLayer);
  L.circleMarker(rider, {radius: 7, color: '#fff', weight: 2, fillColor: css('--rider'),
                         fillOpacity: 1}).addTo(searchLayer);
  let rows = '<tr><th>Chauffeur</th><th>Vehicule</th><th>Distance</th></tr>';
  for (const x of d.results) {
    L.polyline([rider, [x.lat, x.lon]], {color: css('--free'), weight: 2}).addTo(searchLayer);
    rows += `<tr><td>${x.id}</td><td>${x.vehicle}</td><td>${x.dist_km.toFixed(2)} km</td></tr>`;
  }
  $('res').innerHTML = d.results.length ? rows : '';
  $('msg').textContent = d.results.length
    ? `${d.results.length} chauffeur(s) libre(s), modele ${d.model} : ${d.latency_ms} ms, ${d.round_trips} aller(s)-retour(s) vers Redis.`
    : `Aucun chauffeur libre dans le rayon (modele ${d.model}, ${d.latency_ms} ms).`;
  $('btn-reserve').disabled = !d.results.length;
}

map.on('click', e => { rider = e.latlng; search(); });
for (const id of ['radius', 'count', 'vehicle', 'model']) $(id).addEventListener('change', search);

$('btn-reserve').onclick = async () => {
  const p = params();
  const d = await (await fetch('/api/reserve', {method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({lon: rider.lng, lat: rider.lat, radius: p.radius, vehicle: p.vehicle})})).json();
  if (d.driver) {
    reserved = d.driver;
    $('msg').textContent = `Chauffeur ${d.driver} reserve pour ${d.rider} (${d.dist_km} km, ${d.latency_ms} ms). Statut : ${d.fiche.status}.`;
    $('btn-release').disabled = false;
  } else {
    $('msg').textContent = 'Reservation impossible : plus aucun chauffeur libre dans le rayon.';
  }
  await refresh(); await search();
};

$('btn-release').onclick = async () => {
  const d = await (await fetch('/api/release', {method: 'POST',
    headers: {'Content-Type': 'application/json'}, body: JSON.stringify({id: reserved})})).json();
  $('msg').textContent = d.ok ? `Chauffeur ${reserved} libere, de nouveau disponible.`
                              : `Liberation impossible (${d.raison || 'deja libre'}).`;
  reserved = null; $('btn-release').disabled = true;
  await refresh(); await search();
};

async function heat() {
  const d = await (await fetch('/api/heatmap')).json();
  $('heat-hour').textContent = `Heure ${d.heure}, ${d.zones.length} zone(s) avec des demandes.`;
  let rows = '<tr><th>Zone (lat:lon)</th><th>Demandes</th></tr>';
  for (const z of d.zones.slice(0, 8)) rows += `<tr><td>${z.zone}</td><td>${z.n}</td></tr>`;
  $('heat').innerHTML = d.zones.length ? rows : '';
  heatLayer.clearLayers();
  if (!$('show-heat').checked || !d.zones.length) return;
  const max = Math.max(...d.zones.map(z => z.n));
  for (const z of d.zones) {
    L.rectangle([[z.lat, z.lon], [z.lat + d.grid, z.lon + d.grid]],
      {stroke: false, fillColor: css('--busy'), fillOpacity: 0.15 + 0.6 * z.n / max})
      .bindTooltip(`${z.zone} : ${z.n} demande(s)`).addTo(heatLayer);
  }
}
$('show-heat').addEventListener('change', heat);

refresh(); heat();
setInterval(refresh, 2000);
setInterval(heat, 5000);
</script>
</body>
</html>
"""

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
