# Kafka Web API (.NET 8)

A production-ready .NET 8 Web API demonstrating Kafka integration using **Confluent.Kafka**, with resilient HTTP communication via **Polly** and comprehensive error handling.

## Architecture

```
┌──────────────┐      ┌───────────────┐      ┌──────────────────┐
│  HTTP Client │─────>│  Web API      │─────>│  Kafka Broker    │
│  (Swagger)   │      │  (Producer)   │      │  (KRaft mode)    │
└──────────────┘      └───────────────┘      └────────┬─────────┘
                                                      │
                      ┌───────────────┐               │
                      │  Background   │<──────────────┘
                      │  Consumer     │
                      │  + DLT        │──────> Dead Letter Topic
                      │  + Notify     │──────> External Service
                      └───────────────┘        (HttpClient + Polly)
```

## Features

### API Endpoints
- **POST /api/orders** — Create an order and publish it to the `orders` Kafka topic
- **GET /api/orders** — List all orders (including processed ones)
- **GET /api/orders/{id}** — Get a specific order by ID
- **GET /health** — Kafka health check endpoint

### Kafka (Production-Ready)
- **Producer** with idempotent delivery (`Acks.All`), retries, and batching
- **Consumer** with manual offset commits (no auto-commit) for at-least-once delivery
- **Dead-Letter Topic (DLT)** — Failed messages are routed to `orders-dlt` with full context
- **Graceful shutdown** — Final offset commit on application stop
- **Health check** — Verifies Kafka broker connectivity

### HTTP Resilience (Polly)
- **Retry** — 3 retries with exponential backoff (2s, 4s, 8s) for transient HTTP failures
- **Circuit Breaker** — Opens after 5 consecutive failures, stays open for 30s
- **Timeout** — 10s per-request timeout to prevent hanging calls
- **HttpClientFactory** — Proper `HttpClient` lifecycle management (avoids socket exhaustion)

### Other
- **Swagger UI** — Interactive API docs at `/swagger`
- **Docker Compose** — One-command setup with Kafka (KRaft mode, no Zookeeper)

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) & Docker Compose

## Quick Start (Docker Compose)

```bash
docker compose up --build
```

This starts:
- **Kafka broker** on `localhost:9092` (KRaft mode, no Zookeeper needed)
- **Web API** on `http://localhost:5000`

Open Swagger UI: [http://localhost:5000/swagger](http://localhost:5000/swagger)

## Try It Out

### 1. Create an order

```bash
curl -X POST http://localhost:5000/api/orders \
  -H "Content-Type: application/json" \
  -d '{"productName": "Laptop", "quantity": 1, "price": 999.99}'
```

### 2. List all orders

```bash
curl http://localhost:5000/api/orders
```

After the consumer processes the message, the order status changes from `Pending` to `Processed`.

### 3. Check health

```bash
curl http://localhost:5000/health
```

## Local Development (without Docker)

If you want to run the API outside Docker while still using Kafka from Docker:

```bash
# Start only Kafka
docker compose up kafka -d

# Run the API
cd src/KafkaWebApi
dotnet run
```

The API will be available at `http://localhost:5062` (or whatever port .NET assigns).

## Project Structure

```
├── docker-compose.yml                  # Kafka broker + Web API
├── src/KafkaWebApi/
│   ├── Controllers/
│   │   └── OrdersController.cs         # REST endpoints
│   ├── Models/
│   │   ├── Order.cs                    # Order entity + request DTO
│   │   └── KafkaSettings.cs            # Kafka configuration model
│   ├── Resilience/
│   │   └── HttpClientPolicies.cs       # Polly policies (retry, circuit breaker, timeout)
│   ├── Services/
│   │   ├── KafkaProducerService.cs     # Publishes messages to Kafka
│   │   ├── KafkaConsumerService.cs     # Background consumer with DLT + manual commits
│   │   ├── KafkaHealthCheck.cs         # Kafka broker health check
│   │   ├── OrderNotificationService.cs # HTTP notifications with Polly resilience
│   │   └── OrderStore.cs              # Thread-safe in-memory order store
│   ├── Program.cs                      # App startup & DI configuration
│   ├── Dockerfile                      # Multi-stage Docker build
│   └── appsettings.json                # Configuration
└── README.md
```

## Configuration

Kafka settings are configured via `appsettings.json` or environment variables:

| Setting | Default | Env Variable |
|---------|---------|-------------|
| Bootstrap Servers | `localhost:9092` | `Kafka__BootstrapServers` |
| Orders Topic | `orders` | `Kafka__OrdersTopic` |
| Dead Letter Topic | `orders-dlt` | `Kafka__DeadLetterTopic` |
| Consumer Group | `orders-consumer-group` | `Kafka__ConsumerGroupId` |
| Max Retry Attempts | `3` | `Kafka__MaxRetryAttempts` |
| Notification Base URL | `http://localhost:5001` | `Kafka__NotificationBaseUrl` |

## Tech Stack

- **.NET 8** — Latest LTS framework
- **Confluent.Kafka** — Official .NET Kafka client
- **Apache Kafka 3.7** — Event streaming (KRaft mode, no Zookeeper)
- **Polly** — Resilience & transient fault handling (retry, circuit breaker, timeout)
- **HttpClientFactory** — Managed HTTP client lifecycle
- **Swagger / OpenAPI** — Interactive API documentation
- **Health Checks** — Built-in ASP.NET Core health monitoring
