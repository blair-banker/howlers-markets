# Markets Pipeline Makefile

COMPOSE := docker compose -f infrastructure/docker/docker-compose.yml
DATABASE_URL ?= postgresql://postgres:your_password_here@localhost:5432/markets

.PHONY: up down nuke migrate seed-ontology logs psql test test-integration backfill-classify test-stage4

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

backfill-classify:
	@test -n "$(START)" || (echo "Usage: make backfill-classify START=YYYY-MM-DD END=YYYY-MM-DD [EXTRA='--skip-insufficient']" && exit 1)
	@test -n "$(END)"   || (echo "Usage: make backfill-classify START=YYYY-MM-DD END=YYYY-MM-DD [EXTRA='--skip-insufficient']" && exit 1)
	DATABASE_URL=$(DATABASE_URL) python scripts/backfill_classify.py --start $(START) --end $(END) $(EXTRA)

test-stage4:
	pytest packages/markets-core/tests/unit/ \
	       packages/markets-pipeline/tests/unit/test_conditions.py \
	       packages/markets-pipeline/tests/unit/test_transforms_math.py \
	       packages/markets-pipeline/tests/unit/test_rule_based_scoring.py \
	       -v
	DATABASE_URL=$(DATABASE_URL) pytest \
	       packages/markets-pipeline/tests/integration/test_stage4_end_to_end.py \
	       packages/markets-pipeline/tests/contract/test_rule_based_contract.py \
	       packages/markets-pipeline/tests/contract/test_transforms_contract.py \
	       -v -m integration
