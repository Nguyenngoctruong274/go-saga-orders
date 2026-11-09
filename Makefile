.DEFAULT_GOAL := help
DAEMONS := ordersd inventoryd paymentd
export SAGA_KAFKA_BROKERS ?= localhost:19092
PG := postgres://saga:saga@localhost:5432

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build the three daemons into bin/
	@for d in $(DAEMONS); do go build -trimpath -o bin/$$d ./cmd/$$d || exit 1; done

.PHONY: run-orders
run-orders: ## Run ordersd locally on :8080 (needs `make up`)
	SAGA_DATABASE_URL=$(PG)/orders?sslmode=disable go run ./cmd/ordersd

.PHONY: run-inventory
run-inventory: ## Run inventoryd locally on :8081 (needs `make up`)
	SAGA_DATABASE_URL=$(PG)/inventory?sslmode=disable go run ./cmd/inventoryd

.PHONY: run-payment
run-payment: ## Run paymentd locally on :8082 (needs `make up`)
	SAGA_DATABASE_URL=$(PG)/payment?sslmode=disable go run ./cmd/paymentd

.PHONY: test
test: ## Run unit tests with the race detector
	go test -race -count=1 ./...

.PHONY: cover
cover: ## Run tests and print total coverage
	go test -race -coverpkg=./internal/... -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

.PHONY: lint
lint: ## Run golangci-lint
	golangci-lint run

.PHONY: fmt
fmt: ## Format code
	gofmt -s -w .

.PHONY: up
up: ## Start local dependencies (PostgreSQL, Redpanda)
	docker compose up -d

.PHONY: down
down: ## Stop everything and drop the volumes
	docker compose down -v
