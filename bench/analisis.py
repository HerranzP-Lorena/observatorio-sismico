"""Analisis de sismicidad 2025 (terremotos de magnitud >= 4) para las conclusiones del proyecto. Solo lectura.
Cada bloque es una consulta SQL sobre evento_actual; el resultado sale en tablas Markdown.
Uso:  python analisis.py
"""
import asyncio                    # asyncpg es asincrono
import os                         # variables de entorno
from datetime import datetime, timezone

import asyncpg                    # cliente de PostgreSQL

PG = dict(host=os.environ.get("POSTGRES_HOST", "postgres"), user=os.environ["POSTGRES_USER"],
          password=os.environ["POSTGRES_PASSWORD"], database=os.environ["POSTGRES_DB"])
ANYO = int(os.environ.get("HIST_YEAR", "2025"))                       # anyo historico analizado
BASE = "time_utc >= make_timestamptz($1, 1, 1, 0, 0, 0, 'UTC') AND time_utc < make_timestamptz($1 + 1, 1, 1, 0, 0, 0, 'UTC') AND mag >= 4"


def tabla(titulos, filas):
    print("| " + " | ".join(titulos) + " |")
    print("|" + "---|" * len(titulos))
    for f in filas:
        print("| " + " | ".join(str(x) for x in f) + " |")
    print()


async def main():
    db = await asyncpg.connect(**PG)
    n = await db.fetchval("SELECT count(*) FROM evento_actual WHERE " + BASE, ANYO)
    print("# Analisis de sismicidad %d (solo magnitud >= 4)\n" % ANYO)
    print("Fecha: %s UTC. Eventos analizados: %s. Fuente: USGS (catalogo historico, consulta con minmagnitude=4).\n" % (
        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"), "{:,}".format(n)))

    # ---- A. Terremotos por mes: se ve el pico de julio-agosto ----
    print("## A. Terremotos por mes\n")
    f = await db.fetch("""SELECT to_char(date_trunc('month', time_utc AT TIME ZONE 'UTC'), 'YYYY-MM') AS mes, count(*) AS k
                            FROM evento_actual WHERE """ + BASE + " GROUP BY 1 ORDER BY 1", ANYO)
    mx = max((x["k"] for x in f), default=1)
    tabla(["mes", "terremotos", "grafico"], [(x["mes"], "{:,}".format(x["k"]), "#" * round(30 * x["k"] / mx)) for x in f])

    # ---- B. Los 5 dias con mas terremotos y la media frente a la mediana diaria (incluye dias con 0) ----
    print("## B. Dias mas activos y media frente a mediana diaria\n")
    f = await db.fetch("""SELECT (time_utc AT TIME ZONE 'UTC')::date AS dia, count(*) AS k FROM evento_actual
                           WHERE """ + BASE + " GROUP BY 1 ORDER BY k DESC LIMIT 5", ANYO)
    tabla(["dia (UTC)", "terremotos"], [(x["dia"], x["k"]) for x in f])
    r = await db.fetchrow("""WITH dias AS (SELECT d::date AS dia FROM generate_series(make_date($1,1,1), make_date($1,12,31), interval '1 day') d),
                                  c AS (SELECT dias.dia, count(e.event_id) AS k FROM dias
                                          LEFT JOIN evento_actual e ON (e.time_utc AT TIME ZONE 'UTC')::date = dias.dia AND e.mag >= 4
                                         GROUP BY 1)
                             SELECT avg(k) AS media, percentile_cont(0.5) WITHIN GROUP (ORDER BY k) AS mediana,
                                    percentile_cont(0.9) WITHIN GROUP (ORDER BY k) AS p90, max(k) AS maximo, count(*) AS dias FROM c""", ANYO)
    tabla(["estadistico diario (365 dias, incluidos los de 0 eventos)", "valor"], [
        ("media", "%.1f" % r["media"]), ("mediana", "%.1f" % r["mediana"]), ("percentil 90", "%.1f" % r["p90"]), ("maximo", r["maximo"])])

    # ---- C. Magnitud: cada punto mas de magnitud deberia dar ~10 veces menos terremotos (ley de Gutenberg-Richter) ----
    print("## C. Distribucion de magnitud\n")
    f = await db.fetch("""SELECT floor(mag)::int AS m, count(*) AS k FROM evento_actual WHERE """ + BASE + " GROUP BY 1 ORDER BY 1", ANYO)
    filas, prev = [], None
    for x in f:
        filas.append(("M%d - %d,99" % (x["m"], x["m"]), "{:,}".format(x["k"]), "%.1f" % (prev / x["k"]) if prev else "-"))
        prev = x["k"]
    tabla(["tramo", "terremotos", "veces menos que el tramo anterior"], filas)

    # ---- D. Profundidad ----
    print("## D. Distribucion de profundidad\n")
    f = await db.fetch("""SELECT CASE WHEN depth_km < 70 THEN '1 superficial (< 70 km)' WHEN depth_km < 300 THEN '2 intermedia (70-300 km)'
                                      ELSE '3 profunda (>= 300 km)' END AS t, count(*) AS k
                            FROM evento_actual WHERE """ + BASE + " AND depth_km IS NOT NULL GROUP BY 1 ORDER BY 1", ANYO)
    tot = sum(x["k"] for x in f) or 1
    tabla(["tramo", "terremotos", "%"], [(x["t"][2:], "{:,}".format(x["k"]), "%.1f %%" % (100 * x["k"] / tot)) for x in f])

    # ---- E. Paises ----
    print("## E. Paises con mas terremotos y epicentros sin pais\n")
    f = await db.fetch("""SELECT coalesce(p.nombre_es, 'Sin pais asignado') AS pais, count(*) AS k FROM evento_actual e
                            LEFT JOIN pais p ON p.adm0_a3 = e.pais_adm0_a3 WHERE """ + BASE.replace("time_utc", "e.time_utc").replace("mag >=", "e.mag >=") +
                  " GROUP BY 1 ORDER BY k DESC LIMIT 11", ANYO)
    tabla(["pais", "terremotos", "%"], [(x["pais"], "{:,}".format(x["k"]), "%.1f %%" % (100 * x["k"] / n)) for x in f])

    # ---- F. Comprobacion: los 5 paises con mas terremotos, son un enjambre puntual o actividad repartida? ----
    print("## F. Comprobacion de los 5 paises con mas terremotos (excluido 'sin pais')\n")
    filas = []
    tops = await db.fetch("""SELECT p.adm0_a3, p.nombre_es, count(*) AS k FROM evento_actual e JOIN pais p ON p.adm0_a3 = e.pais_adm0_a3
                              WHERE """ + BASE.replace("time_utc", "e.time_utc").replace("mag >=", "e.mag >=") + " GROUP BY 1, 2 ORDER BY k DESC LIMIT 5", ANYO)
    for t in tops:
        m = await db.fetchrow("""SELECT to_char(date_trunc('month', time_utc AT TIME ZONE 'UTC'), 'YYYY-MM') AS mes, count(*) AS k
                                   FROM evento_actual WHERE """ + BASE + " AND pais_adm0_a3 = $2 GROUP BY 1 ORDER BY k DESC LIMIT 1", ANYO, t["adm0_a3"])
        lugar = await db.fetchrow("""SELECT trim(split_part(place, ',', 2)) AS z, count(*) AS k FROM evento_actual
                                      WHERE """ + BASE + " AND pais_adm0_a3 = $2 AND place LIKE '%,%' GROUP BY 1 ORDER BY k DESC LIMIT 1", ANYO, t["adm0_a3"])
        filas.append((t["nombre_es"], "{:,}".format(t["k"]), m["mes"], m["k"], "%.0f %%" % (100 * m["k"] / t["k"]),
                      "%s (%s)" % (lugar["z"], lugar["k"]) if lugar else "-"))
    tabla(["pais", "terremotos", "mes con mas", "en ese mes", "% del total en ese mes", "zona mas frecuente en 'place' (n)"], filas)
    await db.close()


asyncio.run(main())
