# Kafka Web API (.NET 8)

A .NET 8 Web API demonstrating Kafka integration using **Confluent.Kafka**. The project uses an Orders domain to show how a producer publishes messages and a background consumer processes them.

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
                      └───────────────┘
```

## Features

- **POST /api/orders** — Create an order and publish it to the `orders` Kafka topic
- **GET /api/orders** — List all orders (including processed ones)
- **GET /api/orders/{id}** — Get a specific order by ID
- **Kafka Consumer** — Background service that reads from the `orders` topic and marks orders as "Processed"
- **Swagger UI** — Interactive API docs at `/swagger`

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
├── docker-compose.yml              # Kafka broker + Web API
├── src/KafkaWebApi/
│   ├── Controllers/
│   │   └── OrdersController.cs     # REST endpoints
│   ├── Models/
│   │   ├── Order.cs                # Order entity + request DTO
│   │   └── KafkaSettings.cs        # Kafka configuration model
│   ├── Services/
│   │   ├── KafkaProducerService.cs # Publishes messages to Kafka
│   │   ├── KafkaConsumerService.cs # Background consumer
│   │   └── OrderStore.cs           # In-memory order store
│   ├── Program.cs                  # App startup & DI configuration
│   ├── Dockerfile                  # Multi-stage Docker build
│   └── appsettings.json            # Configuration
└── README.md
```

## Configuration

Kafka settings are configured via `appsettings.json` or environment variables:

| Setting | Default | Env Variable |
|---------|---------|-------------|
| Bootstrap Servers | `localhost:9092` | `Kafka__BootstrapServers` |
| Orders Topic | `orders` | `Kafka__OrdersTopic` |
| Consumer Group | `orders-consumer-group` | `Kafka__ConsumerGroupId` |

## Tech Stack

- **.NET 8** — Latest LTS framework
- **Confluent.Kafka** — Official .NET Kafka client
- **Apache Kafka 3.7** — Event streaming (KRaft mode, no Zookeeper)
- **Swagger / OpenAPI** — Interactive API documentation
