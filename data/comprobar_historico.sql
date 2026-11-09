SELECT 'eventos_2025=' || count(*) FROM evento_actual WHERE time_utc >= '2025-01-01' AND time_utc < '2026-01-01';
SELECT 'versiones_2025=' || count(*) FROM evento_version WHERE time_utc >= '2025-01-01' AND time_utc < '2026-01-01';
SELECT 'mag_nula=' || count(*) FILTER (WHERE mag IS NULL) || ' sin_pais=' || count(*) FILTER (WHERE pais_adm0_a3 IS NULL) FROM evento_actual WHERE time_utc >= '2025-01-01' AND time_utc < '2026-01-01';
SELECT 'cuarentena_total=' || count(*) FROM cuarentena;
SELECT 'mes=' || mes || ' n=' || n FROM (SELECT to_char(time_utc at time zone 'UTC','YYYY-MM') AS mes, count(*) AS n FROM evento_actual WHERE time_utc >= '2025-01-01' AND time_utc < '2026-01-01' GROUP BY 1) t ORDER BY mes;
SELECT 'kamchatka mes=' || mes || ' n=' || n FROM (SELECT to_char(time_utc at time zone 'UTC','YYYY-MM') AS mes, count(*) AS n FROM evento_actual WHERE place ILIKE '%Kamchatka%' AND time_utc >= '2025-01-01' AND time_utc < '2026-01-01' GROUP BY 1) t ORDER BY mes;
