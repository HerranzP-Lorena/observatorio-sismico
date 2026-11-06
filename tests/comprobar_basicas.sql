SELECT 'ACTUAL ' || event_id || ' mag=' || coalesce(mag::text,'NULL') || ' prof=' || coalesce(depth_km::text,'NULL')
  || ' pais=' || coalesce(pais_adm0_a3,'NULL') || ' updated=' || to_char(updated_utc at time zone 'UTC','HH24:MI')
FROM evento_actual WHERE event_id LIKE 'test-%' ORDER BY event_id;
SELECT 'VERSIONES ' || event_id || '=' || count(*)
FROM evento_version WHERE event_id LIKE 'test-%' GROUP BY event_id ORDER BY event_id;
SELECT 'CUARENTENA ' || coalesce(event_id,'(sin id)') || ' | ' || categoria || ' | ' || motivo
FROM cuarentena WHERE event_id LIKE 'test-%' OR original::text LIKE '%PRUEBA_SINTETICA%' ORDER BY id;
