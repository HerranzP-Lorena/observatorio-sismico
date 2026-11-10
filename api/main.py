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


# ======================================================================================
#  /api/estadisticas  -  numeros para los graficos del panel
#  Se calcula SIEMPRE desde evento_actual (un registro por terremoto, su ultima version),
#  asi una revision de USGS no cuenta dos veces.
# ======================================================================================
MAX_HORAS = 744          # 31 dias x 24 h: maximo de barras que devuelve "por_hora"
HIST_ANYO = int(os.environ.get("HIST_YEAR", "2025"))   # anyo cargado como historico
HIST_UMBRAL = 4.0        # el historico se descargo con magnitud >= 4 (minmagnitude=4)

# Tramos de los histogramas: (nombre, desde incluido, hasta excluido). None = sin limite.
TRAMOS_MAG = [("menos de 2", None, 2), ("2 a 3", 2, 3), ("3 a 4", 3, 4),
              ("4 a 5", 4, 5), ("5 a 6", 5, 6), ("6 o mas", 6, None)]
TRAMOS_PROF = [("sobre el nivel del mar (<0)", None, 0), ("0 a 10 km", 0, 10), ("10 a 70 km", 10, 70),
               ("70 a 300 km", 70, 300), ("300 km o mas", 300, None)]


def sql_tramos(columna, tramos):
    # Construye "count(*) FILTER (WHERE ...) AS t0, ..." para contar cuantos eventos caen en cada tramo.
    # Los numeros salen de las constantes de arriba (no del usuario), por eso es seguro escribirlos en el SQL.
    # Un NULL no cumple ninguna comparacion, asi que nunca cae en un tramo (se cuenta aparte).
    partes = []
    for i, (_, lo, hi) in enumerate(tramos):
        c = []
        if lo is not None:
            c.append("%s >= %s" % (columna, lo))
        if hi is not None:
            c.append("%s < %s" % (columna, hi))
        partes.append("count(*) FILTER (WHERE %s) AS t%d" % (" AND ".join(c), i))
    return ", ".join(partes)


def histograma(fila, tramos):
    # Convierte la fila de la base en [{"tramo": ..., "n": ...}, ...]
    return [{"tramo": nombre, "n": fila["t%d" % i]} for i, (nombre, _, _) in enumerate(tramos)]


@app.get("/api/estadisticas")
async def estadisticas(
    start: Optional[datetime] = Query(None, description="ISO8601, inclusive. Sin start ni end: ultimas 24 h."),
    end: Optional[datetime] = Query(None, description="ISO8601, exclusive."),
    min_mag: Optional[float] = Query(None, description="Magnitud minima. Los nulos NO cumplen el filtro."),
    country_id: Optional[str] = Query(None, description="ADM0_A3 (p. ej. ESP). 'NONE' = sin pais asignado."),
    top: int = Query(15, ge=1, le=60, description="Cuantos paises se listan; el resto se agrupa en 'otros'."),
):
    """Recuentos por hora UTC y pais, distribuciones de magnitud/profundidad y comparacion con el historico."""
    # --- Mismo rango por defecto y mismos filtros que /api/eventos (para que mapa y graficos coincidan) ---
    ahora = datetime.now(timezone.utc)
    if start is None and end is None:
        start = ahora - timedelta(hours=24)
    if start is not None:
        start = a_utc(start)
    if end is not None:
        end = a_utc(end)
    if start is not None and end is not None and start >= end:
        return JSONResponse(status_code=422, content={"status": "error", "detail": "start debe ser anterior a end"})

    # --- WHERE con parametros. "pais" y "mag" se guardan aparte para poder contar nulos y reutilizar el pais ---
    cond, args = [], []
    def add(sql, valor):
        args.append(valor)
        cond.append(sql.replace("?", "$%d" % len(args)))
    if start is not None:
        add("e.time_utc >= ?", start)
    if end is not None:
        add("e.time_utc < ?", end)
    if country_id:
        if country_id.upper() == "NONE":
            cond.append("e.pais_adm0_a3 IS NULL")
        else:
            add("e.pais_adm0_a3 = ?", country_id.upper())
    donde_sin_mag, args_sin_mag = (" AND ".join(cond) or "TRUE"), list(args)   # filtros SIN magnitud
    if min_mag is not None:
        add("e.mag >= ?", min_mag)
    donde = " AND ".join(cond) or "TRUE"                                       # todos los filtros

    try:
        pool = await get_pool(app)
        async with pool.acquire(timeout=5) as db:
            total = await db.fetchval("SELECT count(*) FROM evento_actual e WHERE " + donde, *args)

            # ---- 1) Recuento por hora UTC (hora del EVENTO, no de llegada) ----
            lo = start or await db.fetchval("SELECT min(e.time_utc) FROM evento_actual e WHERE " + donde, *args)
            hi = end or ahora
            por_hora, aviso_hora = [], None
            if lo is not None:
                lo_h = lo.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)  # redondeo a la hora
                n_horas = int((hi - lo_h).total_seconds() // 3600) + 1
                if n_horas > MAX_HORAS:
                    aviso_hora = "Rango de %d horas: supera el maximo de %d (31 dias). Acota start/end." % (n_horas, MAX_HORAS)
                else:
                    filas = await db.fetch(
                        "SELECT date_trunc('hour', e.time_utc, 'UTC') AS h, count(*) AS n "
                        "FROM evento_actual e WHERE " + donde + " GROUP BY 1", *args)
                    por_h = {r["h"]: r["n"] for r in filas}
                    t = lo_h
                    while t < hi:                       # una barra por hora, tambien las horas con 0 eventos
                        por_hora.append({"hora": t.isoformat(), "n": por_h.get(t, 0),
                                         "parcial": t + timedelta(hours=1) > ahora})   # hora aun en curso
                        t += timedelta(hours=1)

            # ---- 2) Recuento por pais (el NULL se muestra como "Sin pais asignado") ----
            filas = await db.fetch(
                "SELECT e.pais_adm0_a3 AS id, p.nombre_es AS nombre, count(*) AS n "
                "FROM evento_actual e LEFT JOIN pais p ON p.adm0_a3 = e.pais_adm0_a3 "
                "WHERE " + donde + " GROUP BY e.pais_adm0_a3, p.nombre_es "
                "ORDER BY n DESC, e.pais_adm0_a3 NULLS LAST", *args)
            paises = [{"country_id": r["id"], "country_name": r["nombre"] or "Sin país asignado", "n": r["n"]}
                      for r in filas]
            resto = paises[top:]
            paises = paises[:top]

            # ---- 3) Distribucion de magnitud + cuantos eventos no tienen magnitud ----
            fm = await db.fetchrow("SELECT " + sql_tramos("e.mag", TRAMOS_MAG) +
                                   " FROM evento_actual e WHERE " + donde, *args)
            sin_mag = await db.fetchval(
                "SELECT count(*) FROM evento_actual e WHERE e.mag IS NULL AND " + donde_sin_mag, *args_sin_mag)

            # ---- 4) Distribucion de profundidad + cuantos no tienen profundidad ----
            fp = await db.fetchrow("SELECT " + sql_tramos("e.depth_km", TRAMOS_PROF) +
                                   ", count(*) FILTER (WHERE e.depth_km IS NULL) AS sin_valor "
                                   "FROM evento_actual e WHERE " + donde, *args)

            # ---- 5) Comparacion historica: ultimas 24 h vs media diaria del anyo historico ----
            # Solo respeta el filtro de PAIS (y la magnitud minima si es mayor que 4): las ventanas de tiempo son fijas.
            umbral = max(HIST_UMBRAL, min_mag) if min_mag is not None else HIST_UMBRAL
            h_ini = datetime(HIST_ANYO, 1, 1, tzinfo=timezone.utc)
            h_fin = datetime(HIST_ANYO + 1, 1, 1, tzinfo=timezone.utc)
            dias = (h_fin - h_ini).days                          # 365: incluye los dias con 0 eventos
            h_args = [ahora - timedelta(hours=24), ahora, h_ini, h_fin, umbral]
            h_pais, h_pais_args = "", []
            if country_id:
                if country_id.upper() == "NONE":
                    h_pais = " AND e.pais_adm0_a3 IS NULL"
                else:
                    h_pais, h_pais_args = " AND e.pais_adm0_a3 = $6", [country_id.upper()]
            fh = await db.fetchrow(
                "SELECT count(*) FILTER (WHERE e.time_utc >= $1 AND e.time_utc < $2) AS ult24, "
                "count(*) FILTER (WHERE e.time_utc >= $3 AND e.time_utc < $4) AS n_hist "
                "FROM evento_actual e WHERE e.mag >= $5" + h_pais, *(h_args + h_pais_args))
            # Eventos de las ultimas 24 h SIN magnitud: no pueden compararse con ">= 4", se informan aparte
            sin_mag_24 = await db.fetchval(
                "SELECT count(*) FROM evento_actual e WHERE e.mag IS NULL "
                "AND e.time_utc >= $1 AND e.time_utc < $2" + h_pais.replace("$6", "$3"),
                h_args[0], h_args[1], *h_pais_args)
            last = await ultima_ingesta(db)
    except Exception:
        return error_bd()

    # --- Armar la respuesta. Cada bloque dice su unidad, sus filtros y sus nulos ---
    filtros = {"start": start.isoformat() if start else None, "end": end.isoformat() if end else None,
               "min_mag": min_mag, "country_id": country_id}
    periodo = {"start": lo.isoformat() if lo else None, "end": hi.isoformat()}
    media = round(fh["n_hist"] / dias, 3) if fh["n_hist"] > 0 else None   # None (no 0) si el historico no esta cargado
    return {
        "metadata": {
            "generated": now_ms(), "last_ingestion": last, "mode": "live", "total_eventos": total,
            "filters": filtros, "periodo": periodo,
            "fuente": "evento_actual (ultima version de cada terremoto; las revisiones no se cuentan dos veces)",
            "aviso": "El ultimo periodo puede estar incompleto: USGS publica y revisa eventos con retraso.",
        },
        "por_hora": {"unidad": "terremotos por hora UTC (hora del evento)", "filtros": filtros, "periodo": periodo,
                     "aviso": aviso_hora, "datos": por_hora},
        "por_pais": {"unidad": "terremotos por pais", "filtros": filtros, "periodo": periodo,
                     "sin_pais_asignado": next((p["n"] for p in paises + resto if p["country_id"] is None), 0),
                     "otros": {"n_paises": len(resto), "n": sum(p["n"] for p in resto)}, "datos": paises},
        "magnitud": {"unidad": "terremotos por tramo de magnitud", "filtros": filtros, "periodo": periodo,
                     "sin_valor": sin_mag,
                     "nota": "Eventos sin magnitud que cumplen el resto de filtros. Con una magnitud mínima activa no entran en los tramos, pero se siguen contando aquí.",
                     "datos": histograma(fm, TRAMOS_MAG)},
        "profundidad": {"unidad": "terremotos por tramo de profundidad (km)", "filtros": filtros, "periodo": periodo,
                        "sin_valor": fp["sin_valor"], "datos": histograma(fp, TRAMOS_PROF)},
        "comparacion_historica": {
            "unidad": "terremotos por dia con magnitud >= %s" % umbral,
            "filtros": {"country_id": country_id, "min_mag": umbral},
            "periodo_actual": {"start": h_args[0].isoformat(), "end": h_args[1].isoformat()},
            "periodo_historico": {"start": h_ini.isoformat(), "end": h_fin.isoformat(), "dias": dias},
            "ultimas_24h": fh["ult24"], "ultimas_24h_sin_magnitud": sin_mag_24,
            "historico_total": fh["n_hist"], "media_diaria_historica": media,
            "ratio": round(fh["ult24"] / media, 2) if media else None,
            "aviso": ("Historico no cargado: no se puede comparar." if media is None else
                      "La media divide entre %d dias, incluidos los dias con 0 eventos. "
                      "Las ultimas 24 h pueden estar incompletas por retrasos de USGS." % dias),
        },
    }
