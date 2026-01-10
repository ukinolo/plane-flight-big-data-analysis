COMPOSE_FILES = \
	-f base/network.yaml \
	-f hadoop/docker-compose.hadoop.yaml \
	-f nifi/docker-compose.nifi.yaml \
	-f spark/docker-compose.spark.yaml --project-directory .

up:
	docker compose $(COMPOSE_FILES) up -d

down:
	docker compose $(COMPOSE_FILES) down

restart:
	docker compose $(COMPOSE_FILES) down
	docker compose $(COMPOSE_FILES) up -d

ps:
	docker compose $(COMPOSE_FILES) ps

logs:
	docker compose $(COMPOSE_FILES) logs -f

