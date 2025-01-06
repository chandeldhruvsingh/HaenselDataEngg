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
		-v $(PWD)/data:/app/data \
		-v $(PWD)/output:/app/output \
		-e API_KEY="$(API_KEY)" \
		-e CONV_TYPE_ID="$(CONV_TYPE_ID)" \
		-e OUTPUT_PATH="$(OUTPUT_PATH)" \
		-e BATCH_SIZE="$(BATCH_SIZE)" \
		$(IMAGE_NAME)

# Stop and remove the container (if running)
.PHONY: stop
stop:
	docker stop $(CONTAINER_NAME) || true
	docker rm $(CONTAINER_NAME) || true

# Clean up the Docker image
.PHONY: clean
clean:
	docker rmi $(IMAGE_NAME) || true

# Format Python code (optional, if you want to include linting)
.PHONY: format
format:
	black pipeline/*.py

# Test Python code (optional, if you have unit tests)
.PHONY: test
test:
	pytest tests/

# Help target
.PHONY: help
help:
	@echo "Available commands:"
	@echo "  make build     - Build the Docker image"
	@echo "  make run       - Run the Docker container"
	@echo "  make stop      - Stop and remove the Docker container"
	@echo "  make clean     - Remove the Docker image"
	@echo "  make format    - Format Python code with Black"
	@echo "  make test      - Run unit tests"
