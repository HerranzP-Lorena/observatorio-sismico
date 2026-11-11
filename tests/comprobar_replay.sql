-- Comprobaciones de la reproduccion historica. Cada linea de salida es una comprobacion (formato: ETIQUETA|valor).
SELECT 'REPLAY_eventos_actuales|' || count(*) FROM replay.evento_actual;
SELECT 'REPLAY_versiones_guardadas|' || count(*) FROM replay.evento_version;
SELECT 'REPLAY_cuarentena|' || count(*) FROM replay.cuarentena;
-- Aislamiento: ninguna copia sintetica ha llegado a la tabla de datos en vivo, y el historico 2025 en vivo sigue igual
SELECT 'LIVE_con_marca_sintetica|' || count(*) FROM public.evento_actual WHERE place LIKE '%SINTETICA%';
SELECT 'LIVE_eventos_2025|' || count(*) FROM public.evento_actual WHERE time_utc >= '2025-01-01' AND time_utc < '2026-01-01';
-- Caso REVISION: la fila actual debe ser la version posterior (magnitud +0.3)
SELECT 'REVISION_actual|' || event_id || '|mag=' || mag || '|' || place FROM replay.evento_actual WHERE place LIKE '%REVISION SINTETICA%';
-- Todas las versiones de ese evento: la antigua llego tarde y NO debe ser la actual
SELECT 'VERSION|' || v.event_id || '|' || to_char(v.updated_utc, 'YYYY-MM-DD HH24:MI:SS') || '|mag=' || v.mag
  FROM replay.evento_version v
 WHERE v.event_id = (SELECT event_id FROM replay.evento_actual WHERE place LIKE '%REVISION SINTETICA%')
 ORDER BY v.updated_utc;
-- Fecha original conservada y hora de reproduccion anadida (ingested_at = cuando se proceso)
SELECT 'FECHAS|primer evento (original)=' || to_char(min(time_utc), 'YYYY-MM-DD HH24:MI') || '|ultimo (original)=' || to_char(max(time_utc), 'YYYY-MM-DD HH24:MI') || '|reproducido entre ' || to_char(min(ingested_at), 'YYYY-MM-DD HH24:MI:SS') || ' y ' || to_char(max(ingested_at), 'HH24:MI:SS') FROM replay.evento_actual;
