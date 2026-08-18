# Windows equivalent of the Makefile, since `make` is usually absent on Windows.
#
#   .\dev.ps1 up
#   .\dev.ps1 logs auth-api
#   .\dev.ps1 psql auth_db
#   .\dev.ps1 seed

param(
    [Parameter(Position = 0)]
    [ValidateSet('up', 'down', 'clean', 'ps', 'logs', 'topics', 'psql', 'migrate', 'seed', 'test', 'help')]
    [string]$Command = 'help',

    [Parameter(Position = 1)]
    [string]$Target = ''
)

function Show-Help {
    Write-Host ""
    Write-Host "  up               Build and start the whole stack"
    Write-Host "  down             Stop the stack (keeps data)"
    Write-Host "  clean            Stop and DELETE all data volumes"
    Write-Host "  ps               Container status"
    Write-Host "  logs [service]   Tail logs (all, or one service)"
    Write-Host "  topics           List kafka topics"
    Write-Host "  psql [db]        Open psql (default: postgres)"
    Write-Host "  migrate          Run alembic migrations"
    Write-Host "  seed             Load demo products"
    Write-Host "  test             Run the test suite"
    Write-Host ""
}

switch ($Command) {
    'up'      { docker compose up -d --build }
    'down'    { docker compose down }
    'clean'   { docker compose down -v }
    'ps'      { docker compose ps }
    'logs'    {
        if ($Target) { docker compose logs -f --tail=100 $Target }
        else         { docker compose logs -f --tail=100 }
    }
    'topics'  { docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list }
    'psql'    {
        $db = if ($Target) { $Target } else { 'postgres' }
        docker compose exec postgres psql -U app -d $db
    }
    'migrate' {
        docker compose exec auth-api alembic upgrade head
        docker compose exec catalog-api alembic upgrade head
    }
    'seed'    { docker compose exec catalog-api python -m app.seed }
    'test'    {
        docker compose exec auth-api pytest -q
        docker compose exec catalog-api pytest -q
    }
    default   { Show-Help }
}
