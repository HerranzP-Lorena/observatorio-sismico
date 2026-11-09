"""API del observatorio sismico (FastAPI). Solo LEE de PostgreSQL; no escribe nada."""
import os                                   # para leer variables de entorno (usuario, clave...)
import time                                 # para sacar la hora actual
from datetime import datetime, timedelta, timezone  # fechas y horas
from typing import Optional                 # para marcar parametros opcionales
from contextlib import asynccontextmanager  # para definir que hacer al arrancar y al parar

import asyncpg                              # cliente de PostgreSQL (asincrono)
from fastapi import FastAPI, Query          # el framework de la API y los parametros de la URL
from fastapi.middleware.cors import CORSMiddleware  # permite que el navegador llame desde otra web
from fastapi.responses import JSONResponse  # para devolver JSON con el codigo HTTP que queramos

# Datos de conexion a la base. Vienen del compose (que a su vez los lee de .env).
# El host "postgres" es el nombre del servicio en Docker, no "localhost".
PG = dict(
    host=os.environ.get("POSTGRES_HOST", "postgres"),
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
    database=os.environ["POSTGRES_DB"],
)

# CORS: lista de webs autorizadas a llamar a esta API desde el navegador.
# Ahora solo el panel local de Angular; mas adelante se anadira el dominio exacto de Netlify.
CORS_ORIGINS = [o.strip() for o in os.environ.get("CORS_ORIGINS", "http://localhost:4200").split(",") if o.strip()]


@asynccontextmanager
async def lifespan(app):
    # Al ARRANCAR: no se conecta aun a la base (asi la API arranca aunque PostgreSQL tarde).
    app.state.pool = None
    yield
    # Al PARAR: si habia conexiones abiertas, se cierran.
    if app.state.pool:
        await app.state.pool.close()


# Se crea la aplicacion; "lifespan" es lo de arriba (arrancar/parar).
app = FastAPI(title="Observatorio sismico", lifespan=lifespan)

# Se activa CORS: solo se permiten consultas GET y OPTIONS (OPTIONS es la "pregunta previa" del navegador),
# y la cabecera especial de ngrok que usara el panel.
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["ngrok-skip-browser-warning", "content-type"],
)


async def get_pool(app):
    # Un "pool" es un grupo de conexiones a la base reutilizables (mas rapido que abrir una por peticion).
    # Si aun no existe, se crea ahora. Si PostgreSQL esta caido, aqui salta un error.
    if app.state.pool is None:
        app.state.pool = await asyncpg.create_pool(**PG, min_size=1, max_size=5, timeout=5)
    return app.state.pool


def now_ms():
    # Hora actual en milisegundos Unix UTC (el formato que usa el contrato de la API).
    return int(time.time() * 1000)


def error_bd():
    # Respuesta de error EXPLICITO (HTTP 503) cuando la base falla.
    # Nunca se devuelve "0 eventos" como si fuera un resultado valido (lo exige el contrato).
    return JSONResponse(status_code=503, content={"status": "error", "database": "no disponible",
                                                  "generated": now_ms(), "last_ingestion": None})


async def ultima_ingesta(db):
    # Fecha de la ultima consulta a USGS que salio bien (HTTP 200 y sin error), en ms Unix.
    last = await db.fetchval(
        "SELECT max(requested_at) FROM ingesta_consulta WHERE http_status = 200 AND error IS NULL")
    return int(last.timestamp() * 1000) if last else None


def a_utc(d):
    # Si el usuario manda una fecha sin zona horaria, se interpreta como UTC.
    return d.replace(tzinfo=timezone.utc) if d.tzinfo is None else d.astimezone(timezone.utc)


@app.get("/api/salud")   # esta funcion responde a GET /api/salud
async def salud():
    """Estado de la API y fecha de la ultima consulta correcta a USGS. Sin credenciales."""
    try:
        pool = await get_pool(app)
        async with pool.acquire(timeout=5) as db:
            last = await ultima_ingesta(db)
    except Exception:
        return error_bd()
    # Todo bien: estado ok, hora de esta respuesta y hora de la ultima ingesta.
    return {"status": "ok", "database": "ok", "generated": now_ms(), "last_ingestion": last}


# Maximo de puntos por respuesta (el contrato permite limitar si se documenta y se avisa con truncated).
MAX_LIMIT = 5000


@app.get("/api/eventos")   # responde a GET /api/eventos?start=...&end=...&min_mag=...&country_id=...
async def eventos(
    start: Optional[datetime] = Query(None, description="ISO8601, inclusive. Sin start ni end: ultimas 24 h."),
    end: Optional[datetime] = Query(None, description="ISO8601, exclusive."),
    min_mag: Optional[float] = Query(None, description="Magnitud minima. Los nulos NO cumplen el filtro."),
    country_id: Optional[str] = Query(None, description="ADM0_A3 (p. ej. ESP). 'NONE' = sin pais asignado."),
    limit: int = Query(MAX_LIMIT, ge=1, le=MAX_LIMIT, description="Maximo de eventos devueltos."),
    offset: int = Query(0, ge=0, description="Para paginar cuando hay truncado."),
):
    """GeoJSON FeatureCollection con la version ACTUAL de cada terremoto (una sola vez cada uno)."""
    # --- Rango de tiempo ---
    if start is None and end is None:
        # Por defecto (contrato): terremotos ocurridos en las ultimas 24 horas.
        start = datetime.now(timezone.utc) - timedelta(hours=24)
    if start is not None:
        start = a_utc(start)
    if end is not None:
        end = a_utc(end)
    if start is not None and end is not None and start >= end:
        return JSONResponse(status_code=422, content={"status": "error", "detail": "start debe ser anterior a end"})

    # --- Condiciones WHERE construidas con parametros ($1, $2...) para evitar inyeccion SQL ---
    cond, args = [], []
    def add(sql, valor):
        args.append(valor)
        cond.append(sql.replace("?", "$%d" % len(args)))
    if start is not None:
        add("e.time_utc >= ?", start)            # start inclusive
    if end is not None:
        add("e.time_utc < ?", end)               # end exclusivo
    if country_id:
        if country_id.upper() == "NONE":
            cond.append("e.pais_adm0_a3 IS NULL")  # sin pais asignado
        else:
            add("e.pais_adm0_a3 = ?", country_id.upper())
    donde_sin_mag = " AND ".join(cond) or "TRUE"   # filtros SIN el de magnitud (para contar nulos aparte)
    if min_mag is not None:
        add("e.mag >= ?", min_mag)               # un NULL nunca cumple >= (se cuenta aparte)
    donde = " AND ".join(cond) or "TRUE"

    try:
        pool = await get_pool(app)
        async with pool.acquire(timeout=5) as db:
            total = await db.fetchval("SELECT count(*) FROM evento_actual e WHERE " + donde, *args)
            # Cuantos eventos se dejan fuera SOLO por no tener magnitud (se informa, no se esconde)
            nulos_excluidos = 0
            if min_mag is not None:
                nulos_excluidos = await db.fetchval(
                    "SELECT count(*) FROM evento_actual e WHERE e.mag IS NULL AND " + donde_sin_mag,
                    *args[:len(args) - 1])
            filas = await db.fetch(
                "SELECT e.event_id, e.time_utc, e.updated_utc, e.mag, e.mag_type, e.depth_km, e.lon, e.lat, "
                "e.place, e.pais_adm0_a3, p.nombre_es "
                "FROM evento_actual e LEFT JOIN pais p ON p.adm0_a3 = e.pais_adm0_a3 "
                "WHERE " + donde + " ORDER BY e.time_utc DESC, e.event_id "
                "LIMIT $%d OFFSET $%d" % (len(args) + 1, len(args) + 2), *args, limit, offset)
            last = await ultima_ingesta(db)
    except Exception:
        return error_bd()

    # --- Construir el GeoJSON (orden de coordenadas: longitud, latitud, profundidad) ---
    features = [{
        "type": "Feature",
        "id": r["event_id"],
        "geometry": {"type": "Point", "coordinates": [
            r["lon"], r["lat"], float(r["depth_km"]) if r["depth_km"] is not None else None]},
        "properties": {
            "mag": float(r["mag"]) if r["mag"] is not None else None,   # null si no hay magnitud
            "magType": r["mag_type"],
            "place": r["place"],
            "time": int(r["time_utc"].timestamp() * 1000),
            "updated": int(r["updated_utc"].timestamp() * 1000),
            "country_id": r["pais_adm0_a3"],       # null = Sin pais asignado
            "country_name": r["nombre_es"],
        },
    } for r in filas]
    truncado = offset + len(features) < total
    return {
        "type": "FeatureCollection",
        "metadata": {
            "generated": now_ms(), "last_ingestion": last, "mode": "live",
            "count": len(features), "truncated": truncado,
            "total": total, "limit": limit, "offset": offset,
            "next_offset": offset + len(features) if truncado else None,
            "mag_null_excluded": nulos_excluidos,
            "filters": {"start": start.isoformat() if start else None, "end": end.isoformat() if end else None,
                        "min_mag": min_mag, "country_id": country_id},
        },
        "features": features,
    }
