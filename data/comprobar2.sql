SELECT categoria || ' | ' || motivo || ' | veces=' || count(*)
FROM cuarentena GROUP BY categoria, motivo ORDER BY count(*) DESC;
SELECT 'ejemplo: ' || event_id || ' | ' || original::text FROM cuarentena LIMIT 2;
