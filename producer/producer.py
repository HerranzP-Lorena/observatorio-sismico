"""Productor: descarga el feed de USGS y publica en Kafka lo nuevo o revisado."""
import asyncio
import json
import os
import random
from datetime import datetime, timezone

import asyncpg
import httpx
from aiokafka import AIOKafkaProducer

KAFKA = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
TOPIC = os.environ.get("KAFKA_TOPIC", "usgs.live")
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "60"))
USGS_URL = os.environ.get(
    "USGS_URL",
    "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson",
)
MODE = os.environ.get("MODE", "live")
SOURCE = "usgs_all_day"
MAX_TRIES = 5


def log(msg):
    print(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), msg, flush=True)


def ms_to_utc(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


async def connect_db():
    """Conecta a PostgreSQL con reintentos (arrancado no es lo mismo que listo)."""
    for i in range(1, 31):
        try:
            return await asyncpg.connect(
                host=os.environ.get("POSTGRES_HOST", "postgres"),
                user=os.environ["POSTGRES_USER"],
                password=os.environ["POSTGRES_PASSWORD"],
                database=os.environ["POSTGRES_DB"],
            )
        except Exception as e:
            log(f"PostgreSQL no disponible (intento {i}): {e}")
            await asyncio.sleep(min(2 * i, 20))
    raise SystemExit("No se pudo conectar a PostgreSQL")


async def start_kafka():
    for i in range(1, 31):
        producer = AIOKafkaProducer(bootstrap_servers=KAFKA, acks="all")
        try:
            await producer.start()
            return producer
        except Exception as e:
            log(f"Kafka no disponible (intento {i}): {e}")
            await producer.stop()
            await asyncio.sleep(min(2 * i, 20))
    raise SystemExit("No se pudo conectar a Kafka")


async def register_query(db, requested_at, status, n_records, error):
    await db.execute(
        "INSERT INTO ingesta_consulta (source, url, requested_at, http_status, n_records, error) "
        "VALUES ($1, $2, $3, $4, $5, $6)",
        SOURCE, USGS_URL, requested_at, status, n_records, error,
    )


async def fetch_feed(client, db):
    """Descarga el feed con reintentos y espera progresiva. Devuelve el JSON o None."""
    for attempt in range(1, MAX_TRIES + 1):
        requested_at = datetime.now(timezone.utc)
        status, error, data = None, None, None
        try:
            resp = await client.get(USGS_URL)
            status = resp.status_code
            if status == 429 or status >= 500:
                error = f"HTTP {status}"
            elif status != 200:
                error = f"HTTP {status} inesperado"
                await register_query(db, requested_at, status, None, error)
                return None  # un 4xx no se arregla reintentando
            else:
                data = resp.json()
                if not isinstance(data.get("features"), list):
                    error = "JSON sin lista features"
                    data = None
        except httpx.TimeoutException:
            error = "timeout"
        except httpx.HTTPError as e:
            error = f"error de red: {e}"
        except ValueError as e:
            error = f"JSON invalido: {e}"

        n = len(data["features"]) if data else None
        await register_query(db, requested_at, status, n, error)
        if data:
            return data
        wait = min(2 ** attempt, 60) + random.random()
        log(f"Consulta fallida ({error}); reintento {attempt}/{MAX_TRIES} en {wait:.0f} s")
        await asyncio.sleep(wait)
    log("Se agotaron los reintentos; se conserva el estado anterior y se espera al siguiente ciclo")
    return None


async def publish_new(db, producer, data):
    features = data["features"]
    ids = [f.get("id") for f in features if f.get("id")]
    rows = await db.fetch(
        "SELECT event_id, last_updated_published FROM evento_publicado WHERE event_id = ANY($1::text[])",
        ids,
    )
    published = {r["event_id"]: r["last_updated_published"] for r in rows}

    sent = skipped = invalid = 0
    for f in features:
        event_id = f.get("id")
        updated_ms = (f.get("properties") or {}).get("updated")
        if not event_id or not isinstance(updated_ms, int):
            invalid += 1  # sin id no hay clave ni version: se registra, no se publica
            continue
        updated = ms_to_utc(updated_ms)
        prev = published.get(event_id)
        if prev is not None and updated <= prev:
            skipped += 1
            continue
        message = {
            "source": SOURCE,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "mode": MODE,
            "event": f,
        }
        # send_and_wait espera la confirmacion de Kafka antes de seguir
        await producer.send_and_wait(
            TOPIC, key=event_id.encode(), value=json.dumps(message).encode()
        )
        await db.execute(
            "INSERT INTO evento_publicado (event_id, last_updated_published) VALUES ($1, $2) "
            "ON CONFLICT (event_id) DO UPDATE SET last_updated_published = EXCLUDED.last_updated_published "
            "WHERE EXCLUDED.last_updated_published > evento_publicado.last_updated_published",
            event_id, updated,
        )
        sent += 1
    log(f"Feed: {len(features)} eventos | publicados {sent} | ya publicados {skipped} | sin id/updated {invalid}")


async def main():
    db = await connect_db()
    producer = await start_kafka()
    log(f"Productor listo: topic={TOPIC} cada {POLL_SECONDS} s, modo={MODE}")
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        try:
            while True:
                started = asyncio.get_event_loop().time()
                try:
                    data = await fetch_feed(client, db)
                    if data:
                        await publish_new(db, producer, data)
                except Exception as e:  # un fallo de ciclo no debe parar el productor
                    log(f"Error en el ciclo: {e!r}")
                elapsed = asyncio.get_event_loop().time() - started
                await asyncio.sleep(max(1, POLL_SECONDS - elapsed))
        finally:
            await producer.stop()
            await db.close()


if __name__ == "__main__":
    asyncio.run(main())
