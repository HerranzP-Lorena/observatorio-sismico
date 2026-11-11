# Informe de calidad de datos (medido sobre la base real)

Fecha: 2026-11-11 10:39 UTC. Base: tabla evento_actual (un registro por terremoto), 19,287 registros.

## 1. Completitud (campos vacios)

| campo | vacios | % vacios |
|---|---|---|
| mag | 0 | 0.00 % |
| mag_type | 0 | 0.00 % |
| depth_km | 0 | 0.00 % |
| place | 1 | 0.01 % |
| pais_adm0_a3 | 14,515 | 75.26 % |

Nota: 'pais_adm0_a3' vacio = epicentro en el mar o fuera del catalogo 110m (no es un error de USGS).

## 2. Validez (rangos)

| regla | incumplen | % |
|---|---|---|
| magnitud fuera de [-2, 10] | 0 | 0.00 % |
| profundidad negativa (sobre el nivel del mar) | 15 | 0.08 % |
| profundidad > 700 km | 0 | 0.00 % |
| fecha del terremoto en el futuro | 0 | 0.00 % |
| fecha anterior a 1900 | 0 | 0.00 % |

## 3. Unicidad y versiones

| medida | valor |
|---|---|
| ids repetidos en evento_actual (debe ser 0: clave primaria) | 0 |
| versiones guardadas en evento_version | 19,459 |
| terremotos con mas de una version (revisados por USGS) | 83 |
| % de terremotos revisados | 0.43 % |

## 4. Consistencia

| comprobacion | incumplen |
|---|---|
| updated_utc anterior a time_utc (una revision no puede ser previa al terremoto) | 0 |
| evento_actual cuya fecha de version no es la ultima de evento_version | 0 |
| evento_actual sin ninguna version guardada | 0 |
| pais asignado que no existe en la tabla pais | 0 |

## 5. Oportunidad (retraso y consultas a USGS)

| medida (eventos de los ultimos 7 dias, tiempo del terremoto hasta guardarse) | valor |
|---|---|
| eventos considerados | 985 |
| mediana | 803.4 min |
| percentil 90 | 1873.7 min |
| maximo | 2564.1 min |

Nota: esta tabla mezcla el retraso de la tuberia con las horas en que el equipo estuvo apagado (el feed de USGS cubre 24 h, asi que al encender se recuperan los eventos perdidos pero llegan tarde). La tabla siguiente separa ambos efectos.

| eventos que LLEGARON a la base en la ventana | n | mediana | percentil 90 | maximo |
|---|---|---|---|---|
| ultimos 30 min | 5 | 2.8 min | 14.9 min | 22.8 min |
| ultima hora | 11 | 2.8 min | 22.8 min | 57.7 min |
| ultimas 3 horas | 151 | 506.3 min | 1154.0 min | 1412.5 min |
| ultimas 24 horas | 354 | 597.5 min | 1226.2 min | 1414.3 min |

Retraso = (guardado en la base) - (hora del terremoto). Incluye lo que USGS tarda en publicar cada evento, que no controlamos. Ventanas largas se contaminan con paradas del consumidor o del equipo; la ventana corta refleja el funcionamiento normal.

| consultas a USGS registradas | valor |
|---|---|
| consultas | 665 |
| con error o estado distinto de 200 | 0 |
| primera / ultima | 2026-11-05 16:48 / 2026-11-11 10:39 |
| huecos de mas de 5 minutos entre consultas (apagados, reinicios) | 10 |

## 6. Registros rechazados en la entrada (cuarentena)

| categoria | motivo | registros |
|---|---|---|
| excluido | tipo de evento no sismico: 'quarry blast' | 14 |
| excluido | tipo de evento no sismico: 'explosion' | 9 |
| excluido | tipo de evento no sismico: 'ice quake' | 1 |

Total en cuarentena: 24 frente a 19,287 aceptados (0.12 % del total recibido).

