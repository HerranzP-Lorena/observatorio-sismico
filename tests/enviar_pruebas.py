"""Envia mensajes SINTETICOS de prueba a Kafka (ids que empiezan por test-).
Uso: python - basicas   |   python - reinicio
Se ejecuta dentro del contenedor del productor (tiene aiokafka y las variables)."""
import asyncio
import json
import os
import sys
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
TOPIC = os.environ.get("KAFKA_TOPIC", "usgs.live")


def ms(h, m):
    """Milisegundos Unix de 2026-01-15 hh:mm UTC (fecha ficticia, nunca sera 'reciente')."""
    return int(datetime(2026, 1, 15, h, m, tzinfo=timezone.utc).timestamp() * 1000)


def ev(eid, lon, lat, depth, mag, upd_h, upd_m, tipo="earthquake"):
    return {
        "id": eid, "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [lon, lat, depth]},
        "properties": {"type": tipo, "mag": mag, "magType": "ml", "place": "PRUEBA_SINTETICA " + eid,
                       "time": ms(10, 0), "updated": ms(upd_h, upd_m)},
    }


def msg(f):
    return json.dumps({"source": "test_sintetico", "ingested_at": datetime.now(timezone.utc).isoformat(),
                       "mode": "test", "event": f}).encode()


MADRID = (-3.7, 40.4)
ATLANTICO = (-30.0, 40.0)


def conjunto(nombre):
    if nombre == "basicas":
        e1v1 = ev("test-E1", *MADRID, 12, 4.2, 10, 5)
        e1v2 = ev("test-E1", *MADRID, 12, 4.6, 10, 9)
        return [
            ("test-E1", msg(e1v1)),                                   # 1 primera version
            ("test-E1", msg(e1v1)),                                   # 2 repeticion identica
            ("test-E1", msg(e1v2)),                                   # 3 revision posterior
            ("test-E1", msg(e1v1)),                                   # 4 version antigua tarde
            ("test-E2", msg(ev("test-E2", *ATLANTICO, 10, 3.1, 10, 12))),  # 5 otro evento, en el mar
            ("test-MAGNULA", msg(ev("test-MAGNULA", *MADRID, 5, None, 10, 5))),
            ("test-PROFNEG", msg(ev("test-PROFNEG", *MADRID, -1.5, -0.5, 10, 5))),
            ("test-LAT145", msg(ev("test-LAT145", 10.0, 145.0, 5, 3.0, 10, 5))),
            ("test-QUARRY", msg(ev("test-QUARRY", *MADRID, 0, 1.5, 10, 5, tipo="quarry blast"))),
            ("test-BASURA", b"esto no es json PRUEBA_SINTETICA"),
        ]
    if nombre == "reinicio":
        return [("test-E3", msg(ev("test-E3", *MADRID, 8, 2.5, 10, 20)))]
    raise SystemExit("conjunto desconocido: " + nombre)


async def main():
    mensajes = conjunto(sys.argv[1])
    p = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP, acks="all")
    await p.start()
    try:
        for key, value in mensajes:
            r = await p.send_and_wait(TOPIC, key=key.encode(), value=value)
            print(f"enviado {key} -> particion {r.partition} offset {r.offset}", flush=True)
    finally:
        await p.stop()
    print(f"Total enviados: {len(mensajes)}")


if __name__ == "__main__":
    asyncio.run(main())
