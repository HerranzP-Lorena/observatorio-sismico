"""Historico USGS: descarga por meses y publicacion por Kafka.
Uso:  python historico.py descargar   -> baja el anio a ficheros (una sola vez) + manifiesto
      python historico.py publicar    -> publica los eventos en Kafka (mode=historic), ordenados por fecha
"""
import asyncio                      # para programar en asincrono (aiokafka y asyncpg lo necesitan)
import hashlib                      # para calcular la huella SHA-256 de cada fichero descargado
import json                         # leer y escribir JSON
import os                           # variables de entorno y rutas
import random                       # pequena aleatoriedad en la espera entre reintentos
import sys                          # leer el argumento (descargar / publicar)
from datetime import datetime, timedelta, timezone

import httpx                        # cliente HTTP

# ---- Configuracion (por variables de entorno, con valores por defecto del enunciado) ----
YEAR = int(os.environ.get("HIST_YEAR", "2025"))          # anio a descargar
MINMAG = os.environ.get("HIST_MINMAG", "4")              # magnitud minima (decision de alcance)
OUT = os.environ.get("HIST_DIR", "/historico")           # carpeta de trabajo (montada desde el equipo)
DATOS = os.path.join(OUT, "datos")                       # aqui van los ficheros descargados (no van a git)
MANIFEST = os.path.join(OUT, "manifest_%d.json" % YEAR)  # registro de consultas (este SI va a git)
BASE = "https://earthquake.usgs.gov/fdsnws/event/1/query"
MAX_RESULTS = int(os.environ.get("HIST_MAX_RESULTS", "20000"))  # limite de USGS por consulta
PAUSA = float(os.environ.get("HIST_PAUSA", "1"))         # segundos entre consultas (cortesia con USGS)
MAX_TRIES = 5


def log(msg):
    print(datetime.now(timezone.utc).isoformat(timespec="seconds"), msg, flush=True)


def iso(d):
    # Formato de fecha que entiende USGS: 2025-01-01T00:00:00Z
    return d.strftime("%Y-%m-%dT%H:%M:%SZ")


def construir_url(inicio, fin):
    # Misma consulta que el ejemplo del enunciado: geojson, rango, magnitud minima, solo terremotos, por fecha.
    return ("%s?format=geojson&starttime=%s&endtime=%s&minmagnitude=%s&eventtype=earthquake&orderby=time-asc"
            % (BASE, iso(inicio), iso(fin), MINMAG))


async def pedir(client, inicio, fin, registro):
    """Una consulta con reintentos. Devuelve la lista de eventos, o 'DEMASIADOS' si supera el limite de USGS."""
    url = construir_url(inicio, fin)
    for intento in range(1, MAX_TRIES + 1):
        pedida = datetime.now(timezone.utc)
        estado = error = None
        feats = None
        try:
            r = await client.get(url)
            estado = r.status_code
            if estado == 200:
                feats = r.json().get("features")
                if not isinstance(feats, list):
                    error, feats = "JSON sin lista features", None
            elif estado == 400 and "exceeds search limit" in r.text:
                # USGS avisa de que hay mas resultados que el maximo: NO se trunca, se subdivide.
                registro.append(dict(url=url, requested_at=pedida.isoformat(), http_status=estado,
                                     n_records=None, error="supera el limite de %d" % MAX_RESULTS))
                return "DEMASIADOS"
            elif estado == 429 or estado >= 500:
                error = "HTTP %d" % estado          # error temporal: se reintenta
            else:
                error = "HTTP %d inesperado" % estado  # otro 4xx: reintentar no lo arregla
                registro.append(dict(url=url, requested_at=pedida.isoformat(), http_status=estado,
                                     n_records=None, error=error))
                raise SystemExit("Consulta rechazada: %s  %s" % (error, url))
        except httpx.TimeoutException:
            error = "timeout"
        except httpx.HTTPError as e:
            error = "error de red: %s" % e
        except ValueError as e:
            error = "JSON invalido: %s" % e
        registro.append(dict(url=url, requested_at=pedida.isoformat(), http_status=estado,
                             n_records=len(feats) if feats is not None else None, error=error))
        if feats is not None:
            # Seguridad extra: si llega EXACTAMENTE el maximo, podria estar truncado -> subdividir tambien.
            return "DEMASIADOS" if len(feats) >= MAX_RESULTS else feats
        espera = min(2 ** intento, 60) + random.random()
        log("Fallo (%s); reintento %d/%d en %.0f s" % (error, intento, MAX_TRIES, espera))
        await asyncio.sleep(espera)
    raise SystemExit("Se agotaron los reintentos para " + url)


async def intervalo(client, inicio, fin, registro):
    """Descarga [inicio, fin). Si hay demasiados resultados, parte el intervalo por la mitad (recursivo)."""
    res = await pedir(client, inicio, fin, registro)
    await asyncio.sleep(PAUSA)
    if res != "DEMASIADOS":
        return res
    mitad = inicio + (fin - inicio) / 2
    mitad = mitad.replace(microsecond=0)
    if mitad <= inicio:
        raise SystemExit("Intervalo indivisible con demasiados resultados: " + iso(inicio))
    log("Demasiados resultados en %s..%s: se divide en dos" % (iso(inicio), iso(fin)))
    return (await intervalo(client, inicio, mitad, registro)) + (await intervalo(client, mitad, fin, registro))


def fusionar(listas):
    """Une listas de eventos quitando duplicados por id (los limites de USGS pueden incluir ambos extremos).
    Si un id aparece varias veces, se queda con la version de 'updated' mayor. Devuelve ordenado por fecha."""
    por_id = {}
    for feats in listas:
        for f in feats:
            actual = por_id.get(f["id"])
            if actual is None or f["properties"].get("updated", 0) > actual["properties"].get("updated", 0):
                por_id[f["id"]] = f
    return sorted(por_id.values(), key=lambda f: (f["properties"]["time"], f["id"]))


async def descargar():
    os.makedirs(DATOS, exist_ok=True)
    manifiesto = {"anio": YEAR, "filtros": {"minmagnitude": MINMAG, "eventtype": "earthquake", "ambito": "mundial"},
                  "fuente": BASE, "meses": []}
    async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
        for mes in range(1, 13):
            inicio = datetime(YEAR, mes, 1, tzinfo=timezone.utc)
            fin = datetime(YEAR + (mes == 12), mes % 12 + 1, 1, tzinfo=timezone.utc)
            fichero = os.path.join(DATOS, "%d-%02d.geojson" % (YEAR, mes))
            if os.path.exists(fichero):
                # "Descarga una vez y reutiliza los archivos": si ya existe, no se vuelve a pedir.
                log("%s ya existe: se reutiliza" % fichero)
                crudo = open(fichero, "rb").read()
                extraccion = json.loads(crudo.decode("utf-8"))["extraccion"]
                extraccion["sha256"] = hashlib.sha256(crudo).hexdigest()   # huella del fichero tal cual esta
                manifiesto["meses"].append(extraccion)
                continue
            registro = []
            feats = await intervalo(client, inicio, fin, registro)
            extraccion = {"mes": "%d-%02d" % (YEAR, mes), "extraido_en": datetime.now(timezone.utc).isoformat(),
                          "n_registros": len(feats), "consultas": registro}
            contenido = json.dumps({"type": "FeatureCollection", "extraccion": extraccion, "features": feats})
            with open(fichero, "w", encoding="utf-8") as fh:
                fh.write(contenido)
            extraccion["sha256"] = hashlib.sha256(contenido.encode("utf-8")).hexdigest()
            manifiesto["meses"].append(extraccion)
            log("%d-%02d: %d eventos en %d consulta(s)" % (YEAR, mes, len(feats), len(registro)))
    todos = fusionar(json.load(open(os.path.join(DATOS, f), encoding="utf-8"))["features"]
                     for f in sorted(os.listdir(DATOS)) if f.endswith(".geojson"))
    manifiesto["total_bruto"] = sum(m["n_registros"] for m in manifiesto["meses"])
    manifiesto["total_unicos"] = len(todos)
    manifiesto["duplicados_entre_intervalos"] = manifiesto["total_bruto"] - len(todos)
    with open(MANIFEST, "w", encoding="utf-8") as fh:
        json.dump(manifiesto, fh, indent=1, ensure_ascii=False)
    log("Total bruto %d | unicos %d | duplicados entre intervalos %d | manifiesto: %s"
        % (manifiesto["total_bruto"], len(todos), manifiesto["duplicados_entre_intervalos"], MANIFEST))
    await registrar_consultas(manifiesto)


async def registrar_consultas(manifiesto):
    """Guarda cada consulta del historico en la tabla ingesta_consulta (procedencia en la base de datos)."""
    import asyncpg
    try:
        db = await asyncpg.connect(host=os.environ.get("POSTGRES_HOST", "postgres"),
                                   user=os.environ["POSTGRES_USER"], password=os.environ["POSTGRES_PASSWORD"],
                                   database=os.environ["POSTGRES_DB"])
    except Exception as e:
        log("No se pudo registrar en la base (%s); queda el manifiesto" % e)
        return
    ya = await db.fetchval("SELECT count(*) FROM ingesta_consulta WHERE source = 'usgs_historico' AND url LIKE $1",
                           "%starttime=" + str(YEAR) + "%")
    if ya:
        log("Las consultas de %d ya estaban registradas (%d): no se duplican" % (YEAR, ya))
    else:
        filas = [("usgs_historico", c["url"], datetime.fromisoformat(c["requested_at"]), c["http_status"],
                  c["n_records"], c["error"]) for m in manifiesto["meses"] for c in m["consultas"]]
        await db.executemany("INSERT INTO ingesta_consulta (source, url, requested_at, http_status, n_records, error) "
                             "VALUES ($1,$2,$3,$4,$5,$6)", filas)
        log("Registradas %d consultas en ingesta_consulta" % len(filas))
    await db.close()


async def publicar():
    from aiokafka import AIOKafkaProducer
    ficheros = sorted(f for f in os.listdir(DATOS) if f.endswith(".geojson"))
    if not ficheros:
        raise SystemExit("No hay ficheros: ejecuta primero 'descargar'")
    eventos = fusionar(json.load(open(os.path.join(DATOS, f), encoding="utf-8"))["features"] for f in ficheros)
    log("Eventos unicos a publicar: %d (ordenados por fecha de ocurrencia)" % len(eventos))
    p = AIOKafkaProducer(bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"], acks="all")
    await p.start()
    topic = os.environ.get("KAFKA_TOPIC", "usgs.live")
    try:
        for i, f in enumerate(eventos, 1):
            mensaje = {"source": "usgs_historico", "ingested_at": datetime.now(timezone.utc).isoformat(),
                       "mode": "historic", "event": f}
            # Clave = id del evento (como en el productor live); send_and_wait espera la confirmacion de Kafka.
            await p.send_and_wait(topic, key=f["id"].encode(), value=json.dumps(mensaje).encode())
            if i % 1000 == 0:
                log("Publicados %d/%d" % (i, len(eventos)))
    finally:
        await p.stop()
    log("Publicacion terminada: %d mensajes en el topic %s" % (len(eventos), topic))


if __name__ == "__main__":
    accion = sys.argv[1] if len(sys.argv) > 1 else ""
    if accion == "descargar":
        asyncio.run(descargar())
    elif accion == "publicar":
        asyncio.run(publicar())
    else:
        raise SystemExit("Uso: python historico.py descargar | publicar")
