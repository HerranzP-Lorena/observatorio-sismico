SELECT 'actuales=' || (SELECT count(*) FROM evento_actual)
    || ' versiones=' || (SELECT count(*) FROM evento_version)
    || ' cuarentena=' || (SELECT count(*) FROM cuarentena)
    || ' paises=' || (SELECT count(*) FROM pais);
SELECT 'pais=' || coalesce(pais_adm0_a3, 'SIN PAIS') || ' eventos=' || count(*)
FROM evento_actual GROUP BY pais_adm0_a3 ORDER BY count(*) DESC LIMIT 10;
