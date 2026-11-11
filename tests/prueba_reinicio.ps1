# Prueba de aceptacion: "Reiniciar consumidor -> reanuda sin duplicar los efectos ya persistidos"
# Usa UN mensaje SINTETICO (id test-E3, fecha ficticia) y lo borra al terminar.
# Ejecutar desde la carpeta del proyecto:   .\tests\prueba_reinicio.ps1
# Los titulos usan Write-Output (no Write-Host) para que queden en el fichero de evidencia.

# Funcion auxiliar: ejecuta una consulta SQL en PostgreSQL y devuelve solo el valor (sin cabeceras)
function Consulta($sql) {
    $sql | docker compose exec -T postgres psql -U sismo -d sismologia -t -A 2>$null
}

Write-Output "== 0. Estado inicial: filas de test-E3 en evento_actual (debe ser 0)"
Consulta "SELECT count(*) FROM evento_actual WHERE event_id = 'test-E3';"

Write-Output "== 1. Parar el consumidor"
docker compose stop consumer 2>$null | Out-Null
Write-Output "consumidor parado"

Write-Output "== 2. Enviar el mensaje sintetico a Kafka MIENTRAS el consumidor esta parado"
Get-Content tests\enviar_pruebas.py -Raw | docker compose exec -T producer python - reinicio 2>$null
Start-Sleep -Seconds 5

Write-Output "== 3. Con el consumidor parado, el evento NO debe estar en la base (debe ser 0: el mensaje espera en Kafka)"
Consulta "SELECT count(*) FROM evento_actual WHERE event_id = 'test-E3';"

Write-Output "== 4. Arrancar el consumidor y esperar 20 segundos"
docker compose start consumer 2>$null | Out-Null
Start-Sleep -Seconds 20

Write-Output "== 5. Ahora debe aparecer UNA vez: filas en evento_actual y en evento_version (deben ser 1 y 1)"
Consulta "SELECT 'evento_actual=' || count(*) FROM evento_actual WHERE event_id = 'test-E3';"
Consulta "SELECT 'evento_version=' || count(*) FROM evento_version WHERE event_id = 'test-E3';"

Write-Output "== 6. Reiniciar el consumidor otra vez (debe reanudar desde el ultimo offset confirmado) y esperar 20 segundos"
docker compose restart consumer 2>$null | Out-Null
Start-Sleep -Seconds 20

Write-Output "== 7. Tras el reinicio NO se duplica: deben seguir siendo 1 y 1"
Consulta "SELECT 'evento_actual=' || count(*) FROM evento_actual WHERE event_id = 'test-E3';"
Consulta "SELECT 'evento_version=' || count(*) FROM evento_version WHERE event_id = 'test-E3';"

Write-Output "== 8. Posicion del consumidor en Kafka (LAG debe ser 0: no queda nada sin procesar)"
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:19092 --describe --group consumidor-live 2>$null

Write-Output "== 9. Limpieza: borrar el evento sintetico (filas borradas: version, actual)"
Consulta "DELETE FROM evento_version WHERE event_id = 'test-E3';"
Consulta "DELETE FROM evento_actual WHERE event_id = 'test-E3';"
