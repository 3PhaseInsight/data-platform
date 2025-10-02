PROJECT=data-platform
# or env.dev if you prefer
ENV_FILE=.env
COMPOSE_FILES= \
  -f docker-compose.yml \
  -f docker-compose.override.yml \
  -f data-platform-infrastructure/docker-compose.yml \
  -f data-platform-infrastructure/docker-compose.override.yml \
  -f data-platform-frontend/docker-compose.yml \
  -f data-platform-frontend/docker-compose.override.yml

init-db:
	./data-platform-infrastructure/sqitch/create-db-user.sh $(HOST) $(PORT) $(DB_USER) $(PASSWORD) $(ROLE) $(ROLE_PW) $(DB_NAME) && \
  cd data-platform-infrastructure/sqitch && \
	./sqitch-deploy.sh $(HOST):$(PORT) $(DB_USER) $(PASSWORD)

up:
	docker compose --env-file $(ENV_FILE) -p $(PROJECT) $(COMPOSE_FILES) up -d --build && \
  make init-db

down:
	docker compose --env-file $(ENV_FILE) -p $(PROJECT) $(COMPOSE_FILES) down

ps:
	docker compose --env-file $(ENV_FILE) -p $(PROJECT) $(COMPOSE_FILES) ps

logs:
	docker compose --env-file $(ENV_FILE) -p $(PROJECT) $(COMPOSE_FILES) logs -f
  
