"""Reproduccion historica acelerada: publica por Kafka una seleccion del historico 2025, ordenada por fecha.

Va a un topic APARTE (usgs.replay) y el consumidor de reproduccion la guarda en un esquema APARTE (replay),
asi que nunca se mezcla con los datos en vivo. Conserva la fecha original de cada terremoto.
Incluye tres casos controlados (marcados como sinteticos): duplicado, revision y version antigua tardia.

Uso:  python replay.py
Variables: REPLAY_DESDE (fecha ISO UTC de inicio), REPLAY_N (cuantos eventos), REPLAY_PAUSA (segundos entre mensajes)
"""
import asyncio                      # aiokafka es asincrono
import copy                         # para copiar eventos sin modificar el original
import json                         # leer ficheros y construir mensajes
import os                           # variables de entorno y rutas
from datetime import datetime, timezone

# ---- Configuracion (por variables de entorno, con valores por defecto) ----
DATOS = os.path.join(os.environ.get("HIST_DIR", "/historico"), "datos")      # ficheros mensuales ya descargados
TOPIC = os.environ.get("REPLAY_TOPIC", "usgs.replay")                         # topic APARTE del live
DESDE = os.environ.get("REPLAY_DESDE", "2025-07-29T23:00:00Z")                # por defecto: terremoto de Kamchatka
N = int(os.environ.get("REPLAY_N", "40"))                                      # cuantos eventos reales se reproducen
PAUSA = float(os.environ.get("REPLAY_PAUSA", "2"))                             # segundos entre mensajes ("velocidad")
HORA_MS = 3_600_000                                                            # una hora en milisegundos


def log(msg):
    print(datetime.now(timezone.utc).isoformat(timespec="seconds"), msg, flush=True)


def a_ms(texto):
    # "2025-07-29T23:00:00Z" -> milisegundos Unix (el formato de properties.time de USGS)
    return int(datetime.fromisoformat(texto.replace("Z", "+00:00")).timestamp() * 1000)


def cargar_eventos():
    # Lee todos los ficheros mensuales, quita duplicados por id (se queda con la version mas reciente) y ordena por fecha.
    ficheros = sorted(f for f in os.listdir(DATOS) if f.endswith(".geojson"))
    if not ficheros:
        raise SystemExit("No hay ficheros en %s: ejecuta primero la descarga del historico" % DATOS)
    por_id = {}
    for nombre in ficheros:
        with open(os.path.join(DATOS, nombre), encoding="utf-8") as fh:
            for f in json.load(fh)["features"]:
                actual = por_id.get(f["id"])
                if actual is None or f["properties"]["updated"] > actual["properties"]["updated"]:
                    por_id[f["id"]] = f
    return sorted(por_id.values(), key=lambda f: (f["properties"]["time"], f["id"]))


def variante(feature, caso, delta_updated_ms, delta_mag, etiqueta):
    # Copia SINTETICA de un evento real para probar revisiones. Se marca en 'synthetic' y en 'place'.
    f = copy.deepcopy(feature)
    p = f["properties"]
    p["updated"] = p["updated"] + delta_updated_ms                     # fecha de la version (posterior o anterior)
    p["mag"] = round((p["mag"] if isinstance(p.get("mag"), (int, float)) else 4.0) + delta_mag, 1)
    p["place"] = "%s [%s]" % (p.get("place") or "sin descripcion", etiqueta)
    p["synthetic"] = caso
    return f


def construir_secuencia(eventos):
    """Devuelve [(caso, feature), ...] en el orden de envio. Caso: 'real', 'duplicado', 'revision' o 'version_antigua'."""
    base = eventos[:N]
    if len(base) < 5:
        raise SystemExit("Hay menos de 5 eventos desde %s: elige una fecha de inicio anterior" % DESDE)
    seq = []
    for i, f in enumerate(base):
        seq.append(("real", f))
        if i == 4:                                                     # tras el 5.o evento: el MISMO mensaje otra vez
            seq.append(("duplicado", base[4]))
        if i == 9:                                                     # tras el 10.o: revision (version posterior, +0.3 de magnitud)
            seq.append(("revision", variante(base[2], "revision", HORA_MS, +0.3, "REVISION SINTETICA DE PRUEBA")))
        if i == 14:                                                    # tras el 15.o: version ANTIGUA llegando tarde (no debe ganar)
            seq.append(("version_antigua", variante(base[2], "version_antigua", -HORA_MS, -1.0, "VERSION ANTIGUA SINTETICA")))
    return seq


async def main():
    from aiokafka import AIOKafkaProducer
    desde_ms = a_ms(DESDE)
    eventos = [f for f in cargar_eventos() if f["properties"]["time"] >= desde_ms]
    seq = construir_secuencia(eventos)
    log("REPRODUCCION: %d eventos reales + %d casos controlados, desde %s, un mensaje cada %s s -> topic %s"
        % (min(N, len(eventos)), len(seq) - min(N, len(eventos)), DESDE, PAUSA, TOPIC))
    p = AIOKafkaProducer(bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"], acks="all")
    await p.start()
    try:
        for i, (caso, f) in enumerate(seq, 1):
            ahora = datetime.now(timezone.utc)
            mensaje = {"source": "usgs_historico_replay",             # de donde sale
                       "ingested_at": ahora.isoformat(),               # hora de REPRODUCCION (la fecha original va en el evento)
                       "mode": "replay",                               # nunca 'live'
                       "replay_case": caso,                            # real / duplicado / revision / version_antigua
                       "event": f}
            await p.send_and_wait(TOPIC, key=f["id"].encode(), value=json.dumps(mensaje).encode())
            t = datetime.fromtimestamp(f["properties"]["time"] / 1000, tz=timezone.utc).isoformat(timespec="seconds")
            log("[%d/%d] %-15s id=%s fecha original=%s mag=%s" % (i, len(seq), caso, f["id"], t, f["properties"].get("mag")))
            await asyncio.sleep(PAUSA)
    finally:
        await p.stop()
    log("Reproduccion terminada: %d mensajes en %s" % (len(seq), TOPIC))


if __name__ == "__main__":
    asyncio.run(main())
