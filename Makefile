# dev.env is optional — only needed when developing 3phi-framework locally.
# When present and FRAMEWORK_PATH is set, a local wheel is built and installed
# into the dev images. Otherwise, requirements.txt supplies the framework version.
-include dev.env

PROJECT=data-platform
ENV_FILE=.env
COMPOSE_FILES= \
  -f docker-compose.yml \
  -f docker-compose.override.yml \
  -f data-platform-infrastructure/docker-compose.yml \
  -f data-platform-infrastructure/docker-compose.override.yml \
  -f data-platform-frontend/docker-compose.yml \
  -f data-platform-frontend/docker-compose.override.yml

FRAMEWORK_WHEEL:=./tmp/framework_wheel.stamp

ifdef FRAMEWORK_PATH
FRAMEWORK_SRC:=$(shell find $(FRAMEWORK_PATH)/src -type f)
_WHEEL_DEP:=$(FRAMEWORK_WHEEL)
endif

up: $(_WHEEL_DEP)
	docker build -t dask:latest -f Dockerfile.dask .
	docker build -t airflow:latest -f Dockerfile.airflow .
	docker build -t dask:dev -f Dockerfile.dask.dev .
	docker build -t airflow:dev -f Dockerfile.airflow.dev .
	docker compose --env-file $(ENV_FILE) -p $(PROJECT) $(COMPOSE_FILES) up -d --build

down:
	docker compose --env-file $(ENV_FILE) -p $(PROJECT) $(COMPOSE_FILES) down

ps:
	docker compose --env-file $(ENV_FILE) -p $(PROJECT) $(COMPOSE_FILES) ps

logs:
	docker compose --env-file $(ENV_FILE) -p $(PROJECT) $(COMPOSE_FILES) logs -f

$(FRAMEWORK_WHEEL): $(FRAMEWORK_SRC)
	rm -f ./tmp/*
	python -m build $(FRAMEWORK_PATH) --outdir ./tmp/
	touch $(FRAMEWORK_WHEEL)
