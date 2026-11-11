"""Informe de CALIDAD DE DATOS del observatorio (solo lectura: no modifica nada).
Mide seis dimensiones sobre los datos reales ya guardados y escribe un informe en Markdown.
Uso:  python calidad.py
"""
import asyncio                    # asyncpg es asincrono
import os                         # variables de entorno
from datetime import datetime, timezone

import asyncpg                    # cliente de PostgreSQL

PG = dict(host=os.environ.get("POSTGRES_HOST", "postgres"), user=os.environ["POSTGRES_USER"],
          password=os.environ["POSTGRES_PASSWORD"], database=os.environ["POSTGRES_DB"])


def pct(a, b):
    return "%.2f %%" % (100 * a / b) if b else "-"


def tabla(titulos, filas):
    print("| " + " | ".join(titulos) + " |")
    print("|" + "---|" * len(titulos))
    for f in filas:
        print("| " + " | ".join(str(x) for x in f) + " |")
    print()


async def main():
    db = await asyncpg.connect(**PG)
    n = await db.fetchval("SELECT count(*) FROM evento_actual")
    print("# Informe de calidad de datos (medido sobre la base real)\n")
    print("Fecha: %s UTC. Base: tabla evento_actual (un registro por terremoto), %s registros.\n" % (
        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"), "{:,}".format(n)))

    # ---- 1. COMPLETITUD: que porcentaje de cada campo esta vacio ----
    print("## 1. Completitud (campos vacios)\n")
    filas = []
    for campo in ["mag", "mag_type", "depth_km", "place", "pais_adm0_a3"]:
        nulos = await db.fetchval("SELECT count(*) FROM evento_actual WHERE %s IS NULL" % campo)   # campo de una lista fija
        filas.append((campo, "{:,}".format(nulos), pct(nulos, n)))
    tabla(["campo", "vacios", "% vacios"], filas)
    print("Nota: 'pais_adm0_a3' vacio = epicentro en el mar o fuera del catalogo 110m (no es un error de USGS).\n")

    # ---- 2. VALIDEZ: valores dentro de rangos razonables ----
    print("## 2. Validez (rangos)\n")
    reglas = [
        ("magnitud fuera de [-2, 10]", "mag < -2 OR mag > 10"),
        ("profundidad negativa (sobre el nivel del mar)", "depth_km < 0"),
        ("profundidad > 700 km", "depth_km > 700"),
        ("fecha del terremoto en el futuro", "time_utc > now()"),
        ("fecha anterior a 1900", "time_utc < timestamptz '1900-01-01'"),
    ]
    filas = []
    for nombre, cond in reglas:
        k = await db.fetchval("SELECT count(*) FROM evento_actual WHERE " + cond)                 # condiciones fijas de arriba
        filas.append((nombre, "{:,}".format(k), pct(k, n)))
    tabla(["regla", "incumplen", "%"], filas)

    # ---- 3. UNICIDAD: duplicados y revisiones ----
    print("## 3. Unicidad y versiones\n")
    ver = await db.fetchval("SELECT count(*) FROM evento_version")
    multi = await db.fetchval("SELECT count(*) FROM (SELECT event_id FROM evento_version GROUP BY 1 HAVING count(*) > 1) x")
    dup = await db.fetchval("SELECT count(*) - count(DISTINCT event_id) FROM evento_actual")
    tabla(["medida", "valor"], [
        ("ids repetidos en evento_actual (debe ser 0: clave primaria)", dup),
        ("versiones guardadas en evento_version", "{:,}".format(ver)),
        ("terremotos con mas de una version (revisados por USGS)", "{:,}".format(multi)),
        ("% de terremotos revisados", pct(multi, n)),
    ])

    # ---- 4. CONSISTENCIA: coherencia entre campos y entre tablas ----
    print("## 4. Consistencia\n")
    upd = await db.fetchval("SELECT count(*) FROM evento_actual WHERE updated_utc < time_utc")
    desinc = await db.fetchval("""SELECT count(*) FROM evento_actual a
                                   WHERE a.updated_utc <> (SELECT max(v.updated_utc) FROM evento_version v WHERE v.event_id = a.event_id)""")
    sinver = await db.fetchval("SELECT count(*) FROM evento_actual a WHERE NOT EXISTS (SELECT 1 FROM evento_version v WHERE v.event_id = a.event_id)")
    huerf = await db.fetchval("SELECT count(*) FROM evento_actual a WHERE pais_adm0_a3 IS NOT NULL AND NOT EXISTS (SELECT 1 FROM pais p WHERE p.adm0_a3 = a.pais_adm0_a3)")
    tabla(["comprobacion", "incumplen"], [
        ("updated_utc anterior a time_utc (una revision no puede ser previa al terremoto)", upd),
        ("evento_actual cuya fecha de version no es la ultima de evento_version", desinc),
        ("evento_actual sin ninguna version guardada", sinver),
        ("pais asignado que no existe en la tabla pais", huerf),
    ])

    # ---- 5. OPORTUNIDAD: retraso entre el terremoto y su llegada a la base + salud de las consultas ----
    print("## 5. Oportunidad (retraso y consultas a USGS)\n")
    r = await db.fetchrow("""SELECT count(*) AS n,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM ingested_at - time_utc)) AS p50,
            percentile_cont(0.9) WITHIN GROUP (ORDER BY extract(epoch FROM ingested_at - time_utc)) AS p90,
            max(extract(epoch FROM ingested_at - time_utc)) AS mx
        FROM evento_actual WHERE time_utc >= now() - interval '7 days'""")
    if r["n"]:
        tabla(["medida (eventos de los ultimos 7 dias, tiempo del terremoto hasta guardarse)", "valor"], [
            ("eventos considerados", "{:,}".format(r["n"])),
            ("mediana", "%.1f min" % (r["p50"] / 60)),
            ("percentil 90", "%.1f min" % (r["p90"] / 60)),
            ("maximo", "%.1f min" % (r["mx"] / 60)),
        ])
        print("Nota: esta tabla mezcla el retraso de la tuberia con las horas en que el equipo estuvo apagado (el feed de USGS cubre 24 h, "
              "asi que al encender se recuperan los eventos perdidos pero llegan tarde). La tabla siguiente separa ambos efectos.\n")
        # Retraso por VENTANA DE LLEGADA: si el consumidor o el equipo estuvieron parados, los eventos acumulados llegan de golpe y
        # ensucian la mediana global. Mirando solo lo llegado en los ultimos 30 min / 1 h / 3 h se ve el regimen estable.
        # Momento de llegada = primera version guardada de cada evento (evento_version).
        filas = []
        for etiqueta, mins in [("ultimos 30 min", 30), ("ultima hora", 60), ("ultimas 3 horas", 180), ("ultimas 24 horas", 1440)]:
            w = await db.fetchrow("""
                WITH v AS (SELECT event_id, min(ingested_at) AS llegada, min(time_utc) AS t FROM evento_version GROUP BY 1)
                SELECT count(*) AS n,
                       percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM llegada - t)) AS p50,
                       percentile_cont(0.9) WITHIN GROUP (ORDER BY extract(epoch FROM llegada - t)) AS p90,
                       max(extract(epoch FROM llegada - t)) AS mx
                  FROM v WHERE llegada >= now() - make_interval(mins => $1)""", mins)
            if w["n"]:
                filas.append((etiqueta, w["n"], "%.1f min" % (w["p50"] / 60), "%.1f min" % (w["p90"] / 60), "%.1f min" % (w["mx"] / 60)))
            else:
                filas.append((etiqueta, 0, "-", "-", "-"))
        tabla(["eventos que LLEGARON a la base en la ventana", "n", "mediana", "percentil 90", "maximo"], filas)
        print("Retraso = (guardado en la base) - (hora del terremoto). Incluye lo que USGS tarda en publicar cada evento, que no controlamos. "
              "Ventanas largas se contaminan con paradas del consumidor o del equipo; la ventana corta refleja el funcionamiento normal.\n")
    q = await db.fetchrow("""SELECT count(*) AS n, count(*) FILTER (WHERE http_status IS DISTINCT FROM 200 OR error IS NOT NULL) AS fallos,
                                    min(requested_at) AS a, max(requested_at) AS b FROM ingesta_consulta""")
    huecos = await db.fetchval("""SELECT count(*) FROM (SELECT requested_at - lag(requested_at) OVER (ORDER BY requested_at) AS d
                                  FROM ingesta_consulta) x WHERE d > interval '5 minutes'""")
    tabla(["consultas a USGS registradas", "valor"], [
        ("consultas", "{:,}".format(q["n"])), ("con error o estado distinto de 200", "{:,}".format(q["fallos"])),
        ("primera / ultima", "%s / %s" % (q["a"].strftime("%Y-%m-%d %H:%M") if q["a"] else "-", q["b"].strftime("%Y-%m-%d %H:%M") if q["b"] else "-")),
        ("huecos de mas de 5 minutos entre consultas (apagados, reinicios)", huecos),
    ])

    # ---- 6. LO RECHAZADO: cuarentena (lo que no entro en las tablas limpias) ----
    print("## 6. Registros rechazados en la entrada (cuarentena)\n")
    c = await db.fetch("SELECT categoria, motivo, count(*) AS k FROM cuarentena GROUP BY 1, 2 ORDER BY k DESC")
    tot = sum(x["k"] for x in c)
    tabla(["categoria", "motivo", "registros"], [(x["categoria"], x["motivo"][:70], x["k"]) for x in c] or [("-", "ninguno", 0)])
    print("Total en cuarentena: %s frente a %s aceptados (%s del total recibido).\n" % (
        "{:,}".format(tot), "{:,}".format(n), pct(tot, tot + n)))
    await db.close()


asyncio.run(main())
