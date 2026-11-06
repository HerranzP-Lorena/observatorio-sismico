DELETE FROM evento_version WHERE event_id LIKE 'test-%';
DELETE FROM evento_actual  WHERE event_id LIKE 'test-%';
DELETE FROM cuarentena WHERE event_id LIKE 'test-%' OR original::text LIKE '%PRUEBA_SINTETICA%';
SELECT 'restan filas test: ' || ((SELECT count(*) FROM evento_actual WHERE event_id LIKE 'test-%') + (SELECT count(*) FROM evento_version WHERE event_id LIKE 'test-%') + (SELECT count(*) FROM cuarentena WHERE event_id LIKE 'test-%' OR original::text LIKE '%PRUEBA_SINTETICA%'));
