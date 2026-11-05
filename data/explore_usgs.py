import json
import urllib.request
from collections import Counter

URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"

with urllib.request.urlopen(URL, timeout=30) as r:
    print("Código HTTP:", r.status)
    raw = r.read()

# Guardamos el original tal cual (el enunciado exige conservarlo)
with open("usgs_all_day.json", "wb") as f:
    f.write(raw)

data = json.loads(raw)
feats = data["features"]

print("Claves de primer nivel:", list(data.keys()))
print("metadata:", data["metadata"])
print("Número de eventos:", len(feats))
print("Tipos de evento:", Counter(f["properties"]["type"] for f in feats))
print("Magnitud nula:", sum(1 for f in feats if f["properties"]["mag"] is None))
print("place nulo:", sum(1 for f in feats if f["properties"]["place"] is None))
print("magType nulo:", sum(1 for f in feats if f["properties"]["magType"] is None))
print("Profundidad negativa:", sum(1 for f in feats if f["geometry"]["coordinates"][2] < 0))

print("\nPrimer evento completo:")
if len(feats) > 0:
    print(json.dumps(feats[0], indent=2, ensure_ascii=False))
else:
    print("No hay eventos en este momento.")