# Mediciones de rendimiento (datos SINTETICOS, fuera del analisis de sismicidad real)

Fecha: 2026-11-11 10:10 UTC. Semilla aleatoria fija (42): los datos se repiten en cada ejecucion.

## A1. Buscar una version por identificador, en memoria de Python

Registros sinteticos: tupla (event_id, updated_ms, mag, lon, lat). Q = 50 busquedas al azar (coste MEDIO); el PEOR CASO busca el ultimo elemento de la lista.

| N registros | lista: media/busqueda | lista: peor caso | dict: media/busqueda | dict: peor caso | memoria lista | memoria extra del dict |
|---|---|---|---|---|---|---|
| 10,000 | 0.096 ms | 0.397 ms | 0.03 us | 0.04 us | 2.2 MB | 0.2 MB |
| 100,000 | 0.826 ms | 1.655 ms | 0.02 us | 0.04 us | 23.5 MB | 3.7 MB |
| 1,000,000 | 8.324 ms | 15.810 ms | 0.03 us | 0.03 us | 236.8 MB | 29.3 MB |

## B. Integracion geografica: asignar pais a puntos aleatorios (sinteticos)

Catalogo: 177 entidades. 'Uno a uno' = probar cada punto contra los 177 poligonos (O(N*P)); 'STRtree' = indice espacial que usa el consumidor (O(log P) de media por punto).

| N puntos | uno a uno: total | uno a uno: por punto | STRtree: total | STRtree: por punto | mejora | memoria pico STRtree |
|---|---|---|---|---|---|---|
| 1,000 | 0.02 s | 24.3 us | 0.03 s | 25.3 us | x1.0 | 0.1 MB |
| 10,000 | 0.23 s | 23.1 us | 0.26 s | 26.2 us | x0.9 | 0.2 MB |
| 50,000 | 1.09 s | 21.7 us | 1.34 s | 26.8 us | x0.8 | 0.4 MB |

## Equipo y servidor

- Sistema: Linux 6.18.58.1-microsoft-standard-WSL2 (contenedor Docker)
- Python 3.13.18, CPUs visibles: 16
- RAM visible para Docker: 7793.9 MB
- PostgreSQL 16.15 (Debian 16.15-1.pgdg13+2) on x86_64-pc-linux-gnu
- shared_buffers=128MB, work_mem=4MB

## A2. Buscar una version por event_id en PostgreSQL (tabla sintetica, mediana de 50 busquedas; tiempo medido en el cliente, incluye ida y vuelta)

| N registros | SIN indice: mediana | SIN indice: ultimo id | plan sin indice | CON indice: mediana | CON indice: ultimo id | plan con indice | mejora |
|---|---|---|---|---|---|---|---|
| 10,000 | 0.518 ms | 0.630 ms | Seq Scan | 0.079 ms | 0.225 ms | Index Scan | x7 |
| 100,000 | 4.167 ms | 4.182 ms | Seq Scan | 0.082 ms | 0.141 ms | Index Scan | x51 |
| 1,000,000 | 21.769 ms | 19.872 ms | Seq Scan | 0.081 ms | 0.079 ms | Index Scan | x270 |

## C. Agregacion por ventana horaria: contar por hora las ultimas 24 h (mediana de 5 ejecuciones)

| N registros | filas en la ventana | SIN indice en time_utc | plan sin indice | CON indice | plan con indice | mejora |
|---|---|---|---|---|---|---|
| 10,000 | 28 | 0.585 ms | Seq Scan | 0.251 ms | Index Only Scan | x2.3 |
| 100,000 | 274 | 3.571 ms | Seq Scan | 0.199 ms | Index Only Scan | x17.9 |
| 1,000,000 | 2,740 | 16.755 ms | Seq Scan | 0.963 ms | Index Only Scan | x17.4 |

## Almacenamiento y coste de crear los indices (sinteticos)

| N registros | carga de datos | tabla en disco | indices en disco (event_id + time_utc) | tiempo de crear los indices |
|---|---|---|---|---|
| 10,000 | 0.0 s | 1.0 MB | 0.6 MB | 0.01 s + 0.01 s |
| 100,000 | 0.2 s | 9.0 MB | 5.2 MB | 0.04 s + 0.02 s |
| 1,000,000 | 1.7 s | 88.0 MB | 51.5 MB | 0.32 s + 0.18 s |

Esquema bench (datos sinteticos) borrado. Los datos reales no se han tocado.

## Extrapolacion a 100 millones de registros (SUPUESTO LINEAL a partir de 1,000,000 registros: NO medido)

| Magnitud | Medido | Estimado con 100 M | Comentario |
|---|---|---|---|
| Tabla en disco | 88.0 MB | 8.6 GB | crece linealmente con N |
| Indices en disco | 51.5 MB | 5.0 GB | un arbol B por indice |
| Busqueda por id SIN indice | 21.8 ms | 2.2 s | O(N): recorre toda la tabla |
| Busqueda por id CON indice | 0.08 ms | unos pocos ms | O(log N): el arbol gana solo 1-2 niveles |
| Dict de Python (indice en RAM) | 31 bytes/registro | 3.1 GB | no cabe en un portatil: habria que usar el indice de la base de datos |

Memoria maxima del proceso de medicion: 971.2 MB
