# Path to your local multi-repo code directory
DTU_CODE_DIR=../dtu-code
# Variables
REGISTRY := 10.0.1.145:5000
NAMESPACE := 3phi-platform

# Docker images to rebuild
IMAGES=airflow-webserver dask-worker

# Default target
all: build up

init-local:
	mkdir -p volumes/timescale volumes/minio volumes/registry volumes/airflow_logs

# Start or restart the full platform
up:
	@echo "Preparing volumes..."
	make init-local
	@echo "🚀 Starting platform..."
	docker-compose up -d

# Stop everything
down:
	docker-compose down

# Clean volumes (DANGER: data loss)
clean:
	docker-compose down -v

# Run everything in one go
refresh: update-code build up

reload-changes:
	@echo "🚧 Cleaning up old Docker context..."
	rm -rf dags/dtu
	mkdir dags/dtu
	@echo "📦 Copying cleaned repos ..."
	rsync -av --exclude='.git' dtu-code/ dags/dtu
	docker-compose up --build airflow-webserver -d
	docker-compose up --build --scale dask-worker=2 -d
	docker-compose up --build -d airflow-worker

# Default target: print help
.PHONY: help
help:
	@echo "Usage:"
	@echo "  make build <component>"
	@echo "  make push <component>"
	@echo ""
	@echo "Valid components: airflow, dask, timescaledb"

# Build rule with positional argument
.PHONY: build
build:
	@echo "Please provide a component. Example: make build airflow"

build-%:
	DOCKER_BUILDKIT=1 docker build --secret id=pip_conf,src=./pip.conf --platform linux/amd64 -t $(REGISTRY)/$(NAMESPACE)/$*:latest -f Dockerfile.$* .

# Push rule with positional argument
.PHONY: push
push:
	@echo "Please provide a component. Example: make push airflow"

push-%:
	docker push $(REGISTRY)/$(NAMESPACE)/$*:latest

