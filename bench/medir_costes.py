"""Mide el tamano REAL de la base de datos del observatorio (solo lectura: no modifica nada).
Sirve para la seccion de costes: almacenamiento medido por evento y ritmo real de datos.
Uso:  python medir_costes.py
"""
import asyncio                    # asyncpg es asincrono
import os                         # variables de entorno
from datetime import datetime, timezone

import asyncpg                    # cliente de PostgreSQL

PG = dict(host=os.environ.get("POSTGRES_HOST", "postgres"), user=os.environ["POSTGRES_USER"],
          password=os.environ["POSTGRES_PASSWORD"], database=os.environ["POSTGRES_DB"])


def mb(b):
    return "%.2f MB" % (b / 1048576)


async def main():
    db = await asyncpg.connect(**PG)
    print("# Tamano real de la base de datos (medido)\n")
    print("Fecha: %s UTC\n" % datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"))
    print("Base de datos completa en disco: %s\n" % mb(await db.fetchval("SELECT pg_database_size(current_database())")))
    print("| esquema.tabla | filas (exacto) | tamano total (datos + indices) | bytes por fila |")
    print("|---|---|---|---|")
    tablas = await db.fetch("""SELECT n.nspname AS esq, c.relname AS tab, c.oid
                                 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                                WHERE c.relkind = 'r' AND n.nspname IN ('public', 'replay') ORDER BY 1, 2""")
    total_filas = {}
    for t in tablas:
        nombre = '"%s"."%s"' % (t["esq"], t["tab"])           # nombres que salen del propio catalogo de PostgreSQL
        n = await db.fetchval("SELECT count(*) FROM " + nombre)
        tam = await db.fetchval("SELECT pg_total_relation_size($1::oid)", t["oid"])
        total_filas[(t["esq"], t["tab"])] = (n, tam)
        print("| %s.%s | %s | %s | %s |" % (t["esq"], t["tab"], "{:,}".format(n), mb(tam), "%.0f" % (tam / n) if n else "-"))
    live = total_filas.get(("public", "evento_actual"))
    if live and live[0]:
        print()
        v = total_filas.get(("public", "evento_version"), (0, 0))
        por_evento = (live[1] + v[1]) / live[0]
        print("Estimacion: (evento_actual + evento_version) / eventos = %.0f bytes por terremoto" % por_evento)
    rango = await db.fetchrow("SELECT min(time_utc) AS a, max(time_utc) AS b, count(*) AS n FROM public.evento_actual")
    if rango["n"]:
        dias = max((rango["b"] - rango["a"]).total_seconds() / 86400, 1)
        print("\nEventos en evento_actual: %s entre %s y %s (%.1f dias)" % ("{:,}".format(rango["n"]), rango["a"].date(), rango["b"].date(), dias))
        print("Media: %.1f eventos/dia en toda la tabla (incluye historico 2025, no es el ritmo en vivo)" % (rango["n"] / dias))
    await db.close()


asyncio.run(main())
