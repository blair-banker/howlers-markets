# Markets Pipeline Makefile

COMPOSE := docker compose -f infrastructure/docker/docker-compose.yml
DATABASE_URL ?= postgresql://postgres:your_password_here@localhost:5432/markets

.PHONY: up down nuke migrate seed-ontology logs psql test test-integration

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

nuke:
	$(COMPOSE) down -v

migrate:
	alembic -c alembic.ini upgrade head

seed-ontology:
	cat infrastructure/sql/seed/ontology.sql | $(COMPOSE) exec -T timescaledb psql -U postgres -d markets

logs:
	$(COMPOSE) logs -f $(S)

psql:
	$(COMPOSE) exec timescaledb psql -U postgres -d markets

test:
	pytest -v -m "not integration and not slow"

test-integration:
	pytest -v -m integration
