import json
import sys
from collections import Counter

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

RUTA = "frontend/public/datos/paises.geojson"
with open(RUTA, encoding="utf-8") as f:
    data = json.load(f)

feats = data["features"]
print("Claves de primer nivel:", list(data.keys()))
print("crs declarado:", data.get("crs"))
print("Entidades:", len(feats))
print("Tipos de geometria:", dict(Counter(f["geometry"]["type"] for f in feats)))

codes = [f["properties"].get("ADM0_A3") for f in feats]
print("ADM0_A3 distintos:", len(set(codes)), "de", len(codes))
print("ADM0_A3 repetidos:", [c for c, n in Counter(codes).items() if n > 1])
raros = [c for c in codes if not (isinstance(c, str) and len(c) == 3 and c.isalpha() and c.isupper())]
print("ADM0_A3 que no son 3 letras mayusculas:", raros)

for campo in ["ADM0_A3", "NAME_ES", "ADMIN", "CONTINENT", "SUBREGION"]:
    vacios = sum(1 for f in feats if not f["properties"].get(campo))
    print("Vacios en", campo + ":", vacios)

print("Continentes:", dict(Counter(f["properties"].get("CONTINENT") for f in feats)))


def puntos(coords):
    if coords and isinstance(coords[0], (int, float)):
        yield coords
    else:
        for c in coords:
            yield from puntos(c)


lons, lats = [], []
for f in feats:
    for p in puntos(f["geometry"]["coordinates"]):
        lons.append(p[0])
        lats.append(p[1])
print("Longitud min/max:", min(lons), max(lons))
print("Latitud min/max:", min(lats), max(lats))

esp = [f for f in feats if f["properties"].get("ADM0_A3") == "ESP"]
if esp:
    p = esp[0]["properties"]
    g = esp[0]["geometry"]
    print("\nEjemplo ESP:", {k: p.get(k) for k in ["ADM0_A3", "NAME_ES", "ADMIN", "CONTINENT", "SUBREGION"]})
    n = len(g["coordinates"]) if g["type"] == "MultiPolygon" else 1
    print("Geometria:", g["type"], "con", n, "poligono(s)")
