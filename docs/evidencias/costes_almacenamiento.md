# Tamano real de la base de datos (medido)

Fecha: 2026-11-11 10:26 UTC

Base de datos completa en disco: 34.66 MB

| esquema.tabla | filas (exacto) | tamano total (datos + indices) | bytes por fila |
|---|---|---|---|
| public.cuarentena | 24 | 0.09 MB | 4096 |
| public.evento_actual | 19,284 | 4.83 MB | 263 |
| public.evento_publicado | 1,006 | 0.14 MB | 147 |
| public.evento_version | 19,453 | 21.36 MB | 1151 |
| public.ingesta_consulta | 652 | 0.16 MB | 264 |
| public.pais | 177 | 0.06 MB | 370 |
| replay.cuarentena | 0 | 0.02 MB | - |
| replay.evento_actual | 40 | 0.08 MB | 2048 |
| replay.evento_version | 42 | 0.09 MB | 2341 |

Estimacion: (evento_actual + evento_version) / eventos = 1424 bytes por terremoto

Eventos en evento_actual: 19,284 entre 2025-01-01 y 2026-11-11 (679.4 dias)
Media: 28.4 eventos/dia en toda la tabla (incluye historico 2025, no es el ritmo en vivo)
