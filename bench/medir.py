"""Mediciones de rendimiento del observatorio sismico (apartado 10 del enunciado).

TODOS los datos son SINTETICOS y viven en el esquema 'bench' de PostgreSQL (se borra al terminar):
no se mezclan con los terremotos reales. Mide TIEMPO y MEMORIA con tres tamanos de entrada en tres operaciones:
  A) buscar una version por identificador   (lista O(N) vs diccionario O(1) vs SQL sin/con indice)
  B) integracion geografica                 (probar contra los 177 paises uno a uno vs arbol espacial STRtree)
  C) agregacion por ventanas horarias       (SQL sin/con indice sobre el tiempo)
Uso:  python medir.py      (tarda 1-3 minutos)
"""
import asyncio                    # asyncpg es asincrono
import json                       # leer el catalogo de paises y el plan de ejecucion de PostgreSQL
import os                         # variables de entorno
import platform                   # datos del equipo
import random                     # datos sinteticos reproducibles (semilla fija)
import resource                   # memoria maxima del proceso
import statistics                 # mediana
import time                       # cronometro
import tracemalloc                # memoria reservada por Python
from datetime import datetime, timedelta, timezone

import asyncpg                                      # cliente de PostgreSQL
from shapely.geometry import Point, shape           # geometrias
from shapely.strtree import STRtree                 # indice espacial (el mismo que usa el consumidor)

# ---- Configuracion (por variables de entorno, con valores por defecto) ----
SIZES = [int(x) for x in os.environ.get("BENCH_SIZES", "10000,100000,1000000").split(",")]   # tamanos de A y C
GEO = [int(x) for x in os.environ.get("BENCH_GEO", "1000,10000,50000").split(",")]          # tamanos de B (puntos)
CATALOG = os.environ.get("CATALOG_PATH", "/data/paises.geojson")
Q = int(os.environ.get("BENCH_Q", "50"))              # busquedas al azar por medicion
KEEP = os.environ.get("BENCH_KEEP") == "1"            # 1 = no borrar el esquema bench al terminar
PG = dict(host=os.environ.get("POSTGRES_HOST", "postgres"), user=os.environ["POSTGRES_USER"],
          password=os.environ["POSTGRES_PASSWORD"], database=os.environ["POSTGRES_DB"])
random.seed(42)                                       # misma semilla = mismos datos en cada ejecucion


def mb(b):
    return "%.1f MB" % (b / 1024 / 1024)


def ms(s):
    return "%.3f ms" % (s * 1000)


def fila(*celdas):
    print("| " + " | ".join(str(c) for c in celdas) + " |")


def cabecera(*titulos):
    fila(*titulos)
    print("|" + "---|" * len(titulos))


# =====================================================================
#  A1) Python: busqueda por identificador en lista (secuencial) vs diccionario (hash)
# =====================================================================
def medir_python_busqueda():
    print("\n## A1. Buscar una version por identificador, en memoria de Python\n")
    print("Registros sinteticos: tupla (event_id, updated_ms, mag, lon, lat). Q = %d busquedas al azar (coste MEDIO); "
          "el PEOR CASO busca el ultimo elemento de la lista.\n" % Q)
    cabecera("N registros", "lista: media/busqueda", "lista: peor caso", "dict: media/busqueda", "dict: peor caso",
             "memoria lista", "memoria extra del dict")
    for n in SIZES:
        # --- tiempos (sin tracemalloc, que ralentiza) ---
        lista = [("ev%09d" % i, 1_700_000_000_000 + i, 4.0 + (i % 40) / 10, -180 + (i % 3600) / 10, -90 + (i % 1800) / 10)
                 for i in range(n)]
        indice = {r[0]: r for r in lista}
        objetivos = [lista[random.randrange(n)][0] for _ in range(Q)]
        peor = lista[-1][0]

        def secuencial(clave):                    # recorre la lista hasta encontrarlo: O(N)
            for r in lista:
                if r[0] == clave:
                    return r

        t = time.perf_counter(); [secuencial(c) for c in objetivos]; t_lista = (time.perf_counter() - t) / Q
        t = time.perf_counter(); secuencial(peor); t_lista_peor = time.perf_counter() - t
        t = time.perf_counter(); [indice[c] for c in objetivos for _ in range(100)]; t_dict = (time.perf_counter() - t) / (Q * 100)
        t = time.perf_counter(); [indice[peor] for _ in range(100)]; t_dict_peor = (time.perf_counter() - t) / 100
        del lista, indice
        # --- memoria (con tracemalloc, aparte) ---
        tracemalloc.start()
        lista = [("ev%09d" % i, 1_700_000_000_000 + i, 4.0 + (i % 40) / 10, -180 + (i % 3600) / 10, -90 + (i % 1800) / 10)
                 for i in range(n)]
        mem_lista = tracemalloc.get_traced_memory()[0]
        indice = {r[0]: r for r in lista}
        mem_dict = tracemalloc.get_traced_memory()[0] - mem_lista
        tracemalloc.stop()
        del lista, indice
        fila("{:,}".format(n), ms(t_lista), ms(t_lista_peor), "%.2f us" % (t_dict * 1e6), "%.2f us" % (t_dict_peor * 1e6),
             mb(mem_lista), mb(mem_dict))
        RESULTADOS["dict_bytes_por_registro"] = mem_dict / n
        RESULTADOS["lista_ms_1M"] = t_lista * 1000 if n == max(SIZES) else RESULTADOS.get("lista_ms_1M")


# =====================================================================
#  B) Python: integracion geografica (punto en poligono)
# =====================================================================
def medir_geo():
    print("\n## B. Integracion geografica: asignar pais a puntos aleatorios (sinteticos)\n")
    with open(CATALOG, encoding="utf-8") as fh:
        feats = json.load(fh)["features"]
    geoms = [shape(f["geometry"]) for f in feats]
    ids = [f["properties"]["ADM0_A3"] for f in feats]
    tree = STRtree(geoms)
    print("Catalogo: %d entidades. 'Uno a uno' = probar cada punto contra los %d poligonos (O(N*P)); "
          "'STRtree' = indice espacial que usa el consumidor (O(log P) de media por punto).\n" % (len(geoms), len(geoms)))
    cabecera("N puntos", "uno a uno: total", "uno a uno: por punto", "STRtree: total", "STRtree: por punto", "mejora", "memoria pico STRtree")
    for n in GEO:
        pts = [(random.uniform(-180, 180), random.uniform(-85, 85)) for _ in range(n)]
        t = time.perf_counter()
        res_a = []
        for lon, lat in pts:
            p = Point(lon, lat)
            res_a.append(min((ids[i] for i, g in enumerate(geoms) if g.intersects(p)), default=None))
        t_naive = time.perf_counter() - t
        tracemalloc.start()
        t = time.perf_counter()
        res_b = []
        for lon, lat in pts:
            idx = tree.query(Point(lon, lat), predicate="intersects")
            res_b.append(min((ids[i] for i in idx), default=None))
        t_tree = time.perf_counter() - t
        pico = tracemalloc.get_traced_memory()[1]
        tracemalloc.stop()
        assert res_a == res_b, "los dos metodos deberian dar el mismo pais"      # comprobacion de correccion
        sin = sum(1 for r in res_b if r is None)
        fila("{:,}".format(n), "%.2f s" % t_naive, "%.1f us" % (t_naive / n * 1e6), "%.2f s" % t_tree,
             "%.1f us" % (t_tree / n * 1e6), "x%.1f" % (t_naive / t_tree), mb(pico))
        RESULTADOS["geo_sin_pais_pct"] = 100 * sin / n


# =====================================================================
#  A2 + C) PostgreSQL: busqueda por id y agregacion por ventana, sin y con indice
# =====================================================================
async def plan(db, sql, *args):
    # EXPLAIN ANALYZE: devuelve (tipo de nodo principal, tiempo de ejecucion en el servidor en ms)
    r = await db.fetchval("EXPLAIN (ANALYZE, FORMAT JSON) " + sql, *args)
    p = json.loads(r)[0]
    nodo = p["Plan"]
    while "Plans" in nodo and nodo["Node Type"] in ("Limit", "Sort", "Aggregate", "Gather", "Gather Merge", "Finalize GroupAggregate", "Partial Aggregate", "Finalize Aggregate"):
        nodo = nodo["Plans"][0]
    return nodo["Node Type"], p["Execution Time"]


async def mediana_consultas(db, sql, lista_args, repeticiones=1):
    tiempos = []
    for a in lista_args:
        for _ in range(repeticiones):
            t = time.perf_counter(); await db.fetch(sql, *a); tiempos.append(time.perf_counter() - t)
    return statistics.median(tiempos), max(tiempos)


async def medir_sql():
    db = await asyncpg.connect(**PG)
    print("\n## Equipo y servidor\n")
    print("- Sistema: %s %s (contenedor Docker)" % (platform.system(), platform.release()))
    print("- Python %s, CPUs visibles: %s" % (platform.python_version(), os.cpu_count()))
    with open("/proc/meminfo") as fh:
        print("- RAM visible para Docker: %s" % mb(int(fh.readline().split()[1]) * 1024))
    print("- %s" % (await db.fetchval("SELECT version()")).split(",")[0])
    print("- shared_buffers=%s, work_mem=%s" % (await db.fetchval("SHOW shared_buffers"), await db.fetchval("SHOW work_mem")))
    await db.execute("DROP SCHEMA IF EXISTS bench CASCADE")
    await db.execute("CREATE SCHEMA bench")
    filas_a, filas_c, tamanos = [], [], []
    try:
        for n in SIZES:
            tab = "bench.v_%d" % n          # nombre construido solo con un entero: seguro en SQL
            t0 = time.perf_counter()
            await db.execute(f"""
                CREATE TABLE {tab} AS
                SELECT 'ev' || lpad(g::text, 9, '0') AS event_id,
                       timestamptz '2025-01-01' + (g::float8 / {n}) * interval '365 days' AS time_utc,
                       round((random() * 6 + 1)::numeric, 1) AS mag,
                       (random() * 700)::numeric(8,3) AS depth_km,
                       (random() * 360 - 180) AS lon, (random() * 170 - 85) AS lat,
                       (ARRAY['USA','JPN','CHL','IDN','NZL',NULL])[1 + (random() * 5)::int] AS pais
                  FROM generate_series(0, {n} - 1) g""")
            t_carga = time.perf_counter() - t0
            await db.execute(f"ANALYZE {tab}")
            ids = [("ev%09d" % random.randrange(n),) for _ in range(Q)]
            ultimo = [("ev%09d" % (n - 1),)]
            fin = await db.fetchval(f"SELECT max(time_utc) FROM {tab}")
            ventana = (fin - timedelta(hours=24), fin + timedelta(seconds=1))
            q_id = f"SELECT * FROM {tab} WHERE event_id = $1"
            q_ven = (f"SELECT date_trunc('hour', time_utc, 'UTC') AS h, count(*) FROM {tab} "
                     f"WHERE time_utc >= $1 AND time_utc < $2 GROUP BY 1 ORDER BY 1")
            # ---- SIN indice ----
            med_id0, _ = await mediana_consultas(db, q_id, ids)
            _, peor_id0 = await mediana_consultas(db, q_id, ultimo, 3)
            nodo_id0, srv_id0 = await plan(db, q_id, ids[0][0])
            med_v0, _ = await mediana_consultas(db, q_ven, [ventana], 5)
            nodo_v0, srv_v0 = await plan(db, q_ven, *ventana)
            tam_tabla = await db.fetchval(f"SELECT pg_relation_size('{tab}')")
            # ---- CON indice ----
            t0 = time.perf_counter()
            await db.execute(f"CREATE UNIQUE INDEX ON {tab} (event_id)")
            t_idx_id = time.perf_counter() - t0
            t0 = time.perf_counter()
            await db.execute(f"CREATE INDEX ON {tab} (time_utc)")
            t_idx_t = time.perf_counter() - t0
            await db.execute(f"ANALYZE {tab}")
            med_id1, _ = await mediana_consultas(db, q_id, ids)
            _, peor_id1 = await mediana_consultas(db, q_id, ultimo, 3)
            nodo_id1, srv_id1 = await plan(db, q_id, ids[0][0])
            med_v1, _ = await mediana_consultas(db, q_ven, [ventana], 5)
            nodo_v1, srv_v1 = await plan(db, q_ven, *ventana)
            tam_idx = await db.fetchval(f"SELECT pg_indexes_size('{tab}')")
            n_ventana = await db.fetchval(f"SELECT count(*) FROM {tab} WHERE time_utc >= $1 AND time_utc < $2", *ventana)
            filas_a.append(("{:,}".format(n), ms(med_id0), ms(peor_id0), nodo_id0, ms(med_id1), ms(peor_id1), nodo_id1,
                            "x%.0f" % (med_id0 / med_id1)))
            filas_c.append(("{:,}".format(n), "{:,}".format(n_ventana), ms(med_v0), nodo_v0, ms(med_v1), nodo_v1,
                            "x%.1f" % (med_v0 / med_v1)))
            tamanos.append(("{:,}".format(n), "%.1f s" % t_carga, mb(tam_tabla), mb(tam_idx), "%.2f s + %.2f s" % (t_idx_id, t_idx_t)))
            if n == max(SIZES):
                RESULTADOS.update(tabla_mb=tam_tabla / 1048576, idx_mb=tam_idx / 1048576, n_max=n,
                                  sec_ms=med_id0 * 1000, idx_ms=med_id1 * 1000)
        print("\n## A2. Buscar una version por event_id en PostgreSQL (tabla sintetica, mediana de %d busquedas; tiempo medido en el cliente, incluye ida y vuelta)\n" % Q)
        cabecera("N registros", "SIN indice: mediana", "SIN indice: ultimo id", "plan sin indice", "CON indice: mediana", "CON indice: ultimo id", "plan con indice", "mejora")
        for f in filas_a:
            fila(*f)
        print("\n## C. Agregacion por ventana horaria: contar por hora las ultimas 24 h (mediana de 5 ejecuciones)\n")
        cabecera("N registros", "filas en la ventana", "SIN indice en time_utc", "plan sin indice", "CON indice", "plan con indice", "mejora")
        for f in filas_c:
            fila(*f)
        print("\n## Almacenamiento y coste de crear los indices (sinteticos)\n")
        cabecera("N registros", "carga de datos", "tabla en disco", "indices en disco (event_id + time_utc)", "tiempo de crear los indices")
        for f in tamanos:
            fila(*f)
    finally:
        if KEEP:
            print("\n(BENCH_KEEP=1: el esquema bench se conserva)")
        else:
            await db.execute("DROP SCHEMA IF EXISTS bench CASCADE")
            print("\nEsquema bench (datos sinteticos) borrado. Los datos reales no se han tocado.")
        await db.close()


def extrapolar():
    r = RESULTADOS
    if "n_max" not in r:
        return
    f = 100_000_000 / r["n_max"]
    print("\n## Extrapolacion a 100 millones de registros (SUPUESTO LINEAL a partir de %s registros: NO medido)\n" % "{:,}".format(r["n_max"]))
    cabecera("Magnitud", "Medido", "Estimado con 100 M", "Comentario")
    fila("Tabla en disco", mb(r["tabla_mb"] * 1048576), "%.1f GB" % (r["tabla_mb"] * f / 1024), "crece linealmente con N")
    fila("Indices en disco", mb(r["idx_mb"] * 1048576), "%.1f GB" % (r["idx_mb"] * f / 1024), "un arbol B por indice")
    fila("Busqueda por id SIN indice", "%.1f ms" % r["sec_ms"], "%.1f s" % (r["sec_ms"] * f / 1000), "O(N): recorre toda la tabla")
    fila("Busqueda por id CON indice", "%.2f ms" % r["idx_ms"], "unos pocos ms", "O(log N): el arbol gana solo 1-2 niveles")
    fila("Dict de Python (indice en RAM)", "%.0f bytes/registro" % r["dict_bytes_por_registro"],
         "%.1f GB" % (r["dict_bytes_por_registro"] * 100_000_000 / 1e9), "no cabe en un portatil: habria que usar el indice de la base de datos")


RESULTADOS = {}


def main():
    print("# Mediciones de rendimiento (datos SINTETICOS, fuera del analisis de sismicidad real)")
    print("\nFecha: %s UTC. Semilla aleatoria fija (42): los datos se repiten en cada ejecucion." % datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"))
    medir_python_busqueda()
    medir_geo()
    asyncio.run(medir_sql())
    extrapolar()
    print("\nMemoria maxima del proceso de medicion: %s" % mb(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024))


if __name__ == "__main__":
    main()
