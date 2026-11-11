"""Consumidor: Kafka -> validar -> asignar pais -> PostgreSQL (idempotente)."""
import asyncio
import json
import math
import os
import re
import sys
from datetime import datetime, timezone

import asyncpg
from aiokafka import AIOKafkaConsumer
from shapely.geometry import Point, shape
from shapely.strtree import STRtree

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
TOPIC = os.environ.get("KAFKA_TOPIC", "usgs.live")
GROUP = os.environ.get("KAFKA_GROUP", "consumidor-live")
CATALOG = os.environ.get("CATALOG_PATH", "/data/paises.geojson")
# Esquema de PostgreSQL donde escribe este consumidor. 'public' = datos EN VIVO (comportamiento de siempre).
# El consumidor de REPRODUCCION usa DB_SCHEMA=replay: mismas tablas, otro "cajon", sin mezclar con lo real.
SCHEMA = os.environ.get("DB_SCHEMA", "public")
if not re.fullmatch(r"[a-z_][a-z0-9_]*", SCHEMA):      # el nombre se escribe en SQL: solo letras minusculas, numeros y _
    sys.exit(f"DB_SCHEMA invalido: {SCHEMA!r}")
PG = dict(
    host=os.environ.get("POSTGRES_HOST", "postgres"),
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
    database=os.environ["POSTGRES_DB"],
)


def log(msg):
    print(f"{datetime.now(timezone.utc).isoformat(timespec='seconds')} {msg}", flush=True)


def ms_to_utc(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def is_num(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)


# ---------- Catalogo de paises (punto en poligono) ----------
class Catalog:
    def __init__(self, geojson):
        self.rows, geoms = [], []
        for f in geojson["features"]:
            p = f["properties"]
            self.rows.append({
                "adm0_a3": p["ADM0_A3"],
                "nombre_es": p.get("NAME_ES") or p.get("ADMIN") or p["ADM0_A3"],
                "admin": p.get("ADMIN"),
                "continente": p.get("CONTINENT"),
                "subregion": p.get("SUBREGION"),
            })
            geoms.append(shape(f["geometry"]))
        self.tree = STRtree(geoms)

    def country_of(self, lon, lat):
        """ADM0_A3 del poligono que contiene el punto; None si no hay (mar).
        Si hay varias coincidencias (frontera) gana el menor ADM0_A3."""
        idx = self.tree.query(Point(lon, lat), predicate="intersects")
        if len(idx) == 0:
            return None
        return min(self.rows[i]["adm0_a3"] for i in idx)


# ---------- Validacion ----------
def validate(raw):
    """Devuelve (registro, None) si es valido o (None, (categoria, motivo, event_id))."""
    try:
        msg = json.loads(raw)
        f = msg["event"]
        props = f["properties"]
    except Exception:
        return None, ("mensaje_invalido", "JSON ilegible o sin event/properties", None)
    eid = f.get("id")
    if not isinstance(eid, str) or not eid:
        return None, ("mensaje_invalido", "evento sin id", None)
    if props.get("type") != "earthquake":
        return None, ("excluido", f"tipo de evento no sismico: {props.get('type')!r}", eid)
    t, u = props.get("time"), props.get("updated")
    if not (isinstance(t, int) and isinstance(u, int)) or isinstance(t, bool) or isinstance(u, bool):
        return None, ("invalido", "time/updated ausentes o no enteros (ms)", eid)
    try:
        t_utc, u_utc = ms_to_utc(t), ms_to_utc(u)
    except (OverflowError, OSError, ValueError):
        return None, ("invalido", "time/updated fuera de rango", eid)
    geom = f.get("geometry") or {}
    c = geom.get("coordinates")
    if geom.get("type") != "Point" or not isinstance(c, list) or len(c) < 2:
        return None, ("invalido", "geometria no es un Point con coordenadas", eid)
    lon, lat = c[0], c[1]
    depth = c[2] if len(c) > 2 else None
    if not (is_num(lon) and is_num(lat)) or not (-180 <= lon <= 180 and -90 <= lat <= 90):
        return None, ("invalido", f"coordenadas fuera de rango o no numericas: lon={lon!r} lat={lat!r}", eid)
    if depth is not None and not is_num(depth):
        return None, ("invalido", f"profundidad no numerica: {depth!r}", eid)
    mag = props.get("mag")
    if mag is not None and (not is_num(mag) or abs(mag) >= 100):
        return None, ("invalido", f"magnitud no numerica o fuera de rango: {mag!r}", eid)
    return {
        "event_id": eid, "time_utc": t_utc, "updated_utc": u_utc,
        "mag": mag, "mag_type": props.get("magType"), "depth_km": depth,
        "lon": float(lon), "lat": float(lat), "place": props.get("place"),
        "original": json.dumps(f),
    }, None


# ---------- Escritura idempotente ----------
SQL_VERSION = """
INSERT INTO evento_version (event_id, updated_utc, time_utc, mag, mag_type, depth_km,
                            lon, lat, place, pais_adm0_a3, original)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb)
ON CONFLICT (event_id, updated_utc) DO NOTHING"""

SQL_ACTUAL = """
INSERT INTO evento_actual (event_id, time_utc, updated_utc, mag, mag_type, depth_km,
                           lon, lat, place, pais_adm0_a3)
VALUES ($1,$3,$2,$4,$5,$6,$7,$8,$9,$10)
ON CONFLICT (event_id) DO UPDATE SET
  time_utc=EXCLUDED.time_utc, updated_utc=EXCLUDED.updated_utc, mag=EXCLUDED.mag,
  mag_type=EXCLUDED.mag_type, depth_km=EXCLUDED.depth_km, lon=EXCLUDED.lon,
  lat=EXCLUDED.lat, place=EXCLUDED.place, pais_adm0_a3=EXCLUDED.pais_adm0_a3,
  ingested_at=now()
WHERE EXCLUDED.updated_utc > evento_actual.updated_utc"""

SQL_CUARENTENA = """
INSERT INTO cuarentena (event_id, categoria, motivo, original,
                        origen_topic, origen_partition, origen_offset)
VALUES ($1,$2,$3,$4::jsonb,$5,$6,$7)
ON CONFLICT (origen_topic, origen_partition, origen_offset) DO NOTHING"""


async def handle(db, catalog, m, stats):
    rec, rej = validate(m.value)
    if rej:
        cat, motivo, eid = rej
        try:
            original = json.dumps(json.loads(m.value))
        except Exception:
            original = json.dumps({"raw": m.value.decode("utf-8", "replace")})
        await db.execute(SQL_CUARENTENA, eid, cat, motivo, original, m.topic, m.partition, m.offset)
        stats["cuarentena"] += 1
        return
    pais = catalog.country_of(rec["lon"], rec["lat"])
    args = (rec["event_id"], rec["updated_utc"], rec["time_utc"], rec["mag"], rec["mag_type"],
            rec["depth_km"], rec["lon"], rec["lat"], rec["place"], pais)
    async with db.transaction():
        v = await db.execute(SQL_VERSION, *args, rec["original"])
        a = await db.execute(SQL_ACTUAL, *args)
    stats["version_nueva" if v.endswith(" 1") else "version_repetida"] += 1
    if a.endswith(" 1"):
        stats["actual_cambiada"] += 1


async def connect_db():
    for i in range(1, 31):
        try:
            if SCHEMA == "public":
                return await asyncpg.connect(**PG)
            # search_path: primero el esquema propio, luego public (donde vive la tabla compartida 'pais')
            return await asyncpg.connect(**PG, server_settings={"search_path": f"{SCHEMA}, public"})
        except Exception as e:
            log(f"PostgreSQL no disponible ({e!r}); intento {i}/30")
            await asyncio.sleep(2)
    sys.exit("sin PostgreSQL")


TABLAS_POR_ESQUEMA = ["evento_actual", "evento_version", "cuarentena"]   # las que se replican en el esquema propio


async def ensure_schema(db):
    """Crea (si faltan) las tablas del esquema propio como copia de las de 'public'. Idempotente: se puede repetir."""
    if SCHEMA == "public":
        return
    await db.execute(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"')
    for t in TABLAS_POR_ESQUEMA:
        # LIKE ... INCLUDING ALL copia columnas, claves primarias, indices y valores por defecto
        await db.execute(f'CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{t}" (LIKE public.{t} INCLUDING ALL)')
    log(f"Esquema {SCHEMA!r} listo: {', '.join(TABLAS_POR_ESQUEMA)}")


async def main():
    with open(CATALOG, encoding="utf-8") as fh:
        catalog = Catalog(json.load(fh))
    log(f"Catalogo cargado: {len(catalog.rows)} entidades")
    db = await connect_db()
    await ensure_schema(db)
    await db.executemany(
        "INSERT INTO pais (adm0_a3, nombre_es, admin, continente, subregion) VALUES "
        "($1,$2,$3,$4,$5) ON CONFLICT (adm0_a3) DO NOTHING",
        [(r["adm0_a3"], r["nombre_es"], r["admin"], r["continente"], r["subregion"]) for r in catalog.rows])

    consumer = None
    for i in range(1, 31):
        try:
            consumer = AIOKafkaConsumer(
                TOPIC, bootstrap_servers=BOOTSTRAP, group_id=GROUP,
                enable_auto_commit=False, auto_offset_reset="earliest")
            await consumer.start()
            break
        except Exception as e:
            log(f"Kafka no disponible ({e!r}); intento {i}/30")
            await asyncio.sleep(2)
    else:
        sys.exit("sin Kafka")
    log(f"Consumidor listo: topic={TOPIC} grupo={GROUP} esquema={SCHEMA}")

    stats = dict(version_nueva=0, version_repetida=0, actual_cambiada=0, cuarentena=0)
    last_log = asyncio.get_event_loop().time()
    try:
        async for m in consumer:
            try:
                await handle(db, catalog, m, stats)
            except (asyncpg.PostgresConnectionError, asyncpg.InterfaceError, OSError) as e:
                log(f"Error de BD ({e!r}); se sale SIN confirmar offset para reintentar al reiniciar")
                sys.exit(1)
            await consumer.commit()  # offset confirmado DESPUES de la transaccion
            now = asyncio.get_event_loop().time()
            if now - last_log > 10:
                log(f"Acumulado: {stats}")
                last_log = now
    finally:
        log(f"Final: {stats}")
        await consumer.stop()
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
