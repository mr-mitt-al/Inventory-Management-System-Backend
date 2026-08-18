.DEFAULT_GOAL := help
COMPOSE := docker compose
S ?=

.PHONY: help up down clean logs ps seed migrate test lint topics psql

help:  ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

up:  ## Build and start the whole stack
	$(COMPOSE) up -d --build

down:  ## Stop the stack (keeps data)
	$(COMPOSE) down

clean:  ## Stop and DELETE all data volumes (admin re-seeds on next up)
	$(COMPOSE) down -v

ps:  ## Show container status
	$(COMPOSE) ps

logs:  ## Tail logs. Usage: make logs S=auth-api
	@if [ -z "$(S)" ]; then $(COMPOSE) logs -f --tail=100; \
	else $(COMPOSE) logs -f --tail=100 $(S); fi

topics:  ## List kafka topics
	$(COMPOSE) exec kafka kafka-topics --bootstrap-server localhost:9092 --list

psql:  ## Open psql. Usage: make psql S=auth_db
	$(COMPOSE) exec postgres psql -U app -d $(or $(S),postgres)

migrate:  ## Run alembic migrations for every service
	$(COMPOSE) exec auth-api alembic upgrade head
	$(COMPOSE) exec catalog-api alembic upgrade head

seed:  ## Load demo categories, products and stock
	$(COMPOSE) exec catalog-api python -m app.seed

test:  ## Run the test suite
	$(COMPOSE) exec auth-api pytest -q
	$(COMPOSE) exec catalog-api pytest -q

lint:  ## Ruff check across the repo
	ruff check common services
