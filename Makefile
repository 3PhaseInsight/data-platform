PROJECT=data-platform
ENV_FILE=.env
COMPOSE_FILES= \
  -f docker-compose.yml \
  -f docker-compose.override.yml \
  -f data-platform-infrastructure/docker-compose.yml \
  -f data-platform-infrastructure/docker-compose.override.yml \
  -f data-platform-frontend/docker-compose.yml \
  -f data-platform-frontend/docker-compose.override.yml

up:
	docker compose --env-file $(ENV_FILE) -p $(PROJECT) $(COMPOSE_FILES) up -d --build

down:
	docker compose --env-file $(ENV_FILE) -p $(PROJECT) $(COMPOSE_FILES) down

ps:
	docker compose --env-file $(ENV_FILE) -p $(PROJECT) $(COMPOSE_FILES) ps

logs:
	docker compose --env-file $(ENV_FILE) -p $(PROJECT) $(COMPOSE_FILES) logs -f
  
