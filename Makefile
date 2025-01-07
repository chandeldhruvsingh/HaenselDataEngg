# Variables
IMAGE_NAME = attribution_pipeline
CONTAINER_NAME = attribution_pipeline_container
DOCKERFILE = Dockerfile

# Default target
.PHONY: all
all: build run

# Build the Docker image
.PHONY: build
build:
	docker build -t $(IMAGE_NAME) -f $(DOCKERFILE) .

# Run the Docker container
.PHONY: run
run:
	docker run --rm \
		--name $(CONTAINER_NAME) \
		$(IMAGE_NAME) $(if $(START_DATE),--start_date=$(START_DATE)) $(if $(END_DATE),--end_date=$(END_DATE))

.PHONY: stop
stop:
	docker stop $(CONTAINER_NAME) || true
	docker rm $(CONTAINER_NAME) || true

.PHONY: clean
clean:
	docker rmi $(IMAGE_NAME) || true

.PHONY: test
test:
	pytest tests/

# Help target
.PHONY: help
help:
	@echo "Available commands:"
	@echo "  make build                                                        - Build the Docker image"
	@echo "  make run                                                          - Run the Docker container"
	@echo "  make run START_DATE=2023-01-07 END_DATE=2023-02-21                - Optionally pass START_DATE and END_DATE for a date range"
	@echo "  make stop                                                         - Stop and remove the Docker container"
	@echo "  make clean                                                        - Remove the Docker image"
	@echo "  make format                                                       - Format Python code with Black"
	@echo "  make test                                                         - Run unit tests"
