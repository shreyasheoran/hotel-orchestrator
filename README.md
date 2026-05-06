# Hotel Offer Orchestrator

A production-style Node.js/TypeScript service that aggregates hotel offers from two mock suppliers, deduplicates listings (cheapest price wins), caches results in Redis, and exposes a filterable REST API. Orchestration is handled by **Temporal.io** for reliable, retryable workflow execution.

---

## Table of Contents

- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Quick Start](#quick-start)
- [Environment Variables](#environment-variables)
- [API Reference](#api-reference)
- [How It Works](#how-it-works)
- [Supported Cities & Data](#supported-cities--data)
- [Postman Collection](#postman-collection)
- [Stopping the Stack](#stopping-the-stack)

---

## Architecture

```
Client Request
      │
      ▼
┌─────────────────────┐
│   Express API       │──── Cache HIT ────▶ Redis (Sorted Set, score = price)
│   GET /api/hotels   │                          │
└─────────────────────┘                          │ zRangeByScore (price filter)
      │                                          ▼
      │ Cache MISS                     Filtered results → Client
      ▼
┌──────────────────────────────────────────────┐
│         Temporal Workflow                    │
│                                              │
│  ┌─────────────────┐  ┌─────────────────┐    │
│  │  fetchSupplierA │  │  fetchSupplierB │    │  ← Parallel HTTP calls
│  └────────┬────────┘  └────────┬────────┘    │
│           └──────────┬─────────┘             │
│                      ▼                       │
│           Deduplicate by name                │
│           (cheapest price wins)              │
│                      │                       │
│                      ▼                       │
│              saveToRedis (TTL = 5 min)       │
└──────────────────────────────────────────────┘
                       │
                       ▼
             Filtered results → Client
```

**Key design decisions:**

- Redis **Sorted Set** with price as score enables `ZRANGEBYSCORE` for O(log N) price filtering — no in-memory iteration on cache hits
- **Sentinel keys** prevent repeated Temporal workflows for cities with no results
- Temporal activities **retry up to 3 times** with a 1-minute timeout per activity
- Both the API server and the worker use **connection retry loops** to tolerate Docker startup ordering delays

---

## Tech Stack

| Layer                   | Technology              |
| ----------------------- | ----------------------- |
| Runtime                 | Node.js 20 (TypeScript) |
| API Server              | Express.js              |
| Workflow Orchestration  | Temporal.io             |
| Cache & Price Filtering | Redis 7 (Sorted Sets)   |
| Temporal Backend        | PostgreSQL 12           |
| Containerisation        | Docker + Docker Compose |

---

## Project Structure

```
hotel-orchestrator/
├── src/
│   ├── config.ts          # All env vars & constants — single source of truth
│   ├── redis.ts           # Shared Redis client factory + key helpers
│   ├── supplierData.ts    # Multi-city mock hotel data for both suppliers
│   ├── activities.ts      # Temporal activities: fetch suppliers, save to Redis
│   ├── workflows.ts       # Temporal workflow: parallel fetch → dedup → cache
│   ├── worker.ts          # Temporal worker process
│   └── server.ts          # Express API + mock supplier endpoints
├── Dockerfile
├── docker-compose.yml
├── hotel-orchestrator_postman_collection.json
├── package.json
└── tsconfig.json
```

---

## Quick Start

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) ≥ 24
- [Docker Compose](https://docs.docker.com/compose/) v2 (included with Docker Desktop)

No Node.js installation is required on the host — everything runs inside containers.

### 1. Clone the repository

```bash
git clone https://github.com/shreyasheoran/hotel-orchestrator.git
cd hotel-orchestrator
```

### 2. Start all services

```bash
docker compose up --build
```

This starts: **Redis**, **PostgreSQL**, **Temporal**, **Temporal UI**, **API server**, and **Worker** — in the correct dependency order, with health checks.

### 3. Wait ~20 seconds for Temporal to initialise, then verify

```bash
# Health check — both suppliers should report "up"
curl http://localhost:3000/health

# First call: triggers the Temporal workflow, caches results
curl "http://localhost:3000/api/hotels?city=delhi"

# Second call: served instantly from Redis cache
curl "http://localhost:3000/api/hotels?city=delhi"
```

The **Temporal UI** is available at **http://localhost:8233** to inspect workflow executions visually.

### Local development (app outside Docker)

```bash
# Start only the infrastructure services
docker compose up redis postgresql temporal temporal-ui -d

# Install dependencies
npm install

# Start the Temporal worker (keep this running in a separate terminal)
npm run start:worker

# Start the API server
npm run start:server
```

---

## Environment Variables

All configuration is environment-driven. Defaults are set in `docker-compose.yml`; override them there or via a `.env` file.

| Variable                     | Default (Docker)     | Description                                                                   |
| ---------------------------- | -------------------- | ----------------------------------------------------------------------------- |
| `PORT`                       | `3000`               | API server port                                                               |
| `REDIS_URL`                  | `redis://redis:6379` | Redis connection string                                                       |
| `REDIS_TTL_SECONDS`          | `300`                | Cache TTL for city result sets (5 min)                                        |
| `REDIS_SENTINEL_TTL_SECONDS` | `60`                 | TTL for unknown-city sentinel keys (1 min)                                    |
| `REDIS_HOTEL_KEY_PREFIX`     | `hotels`             | Prefix for all Redis keys                                                     |
| `TEMPORAL_ADDRESS`           | `temporal:7233`      | Temporal server gRPC address                                                  |
| `TEMPORAL_TASK_QUEUE`        | `hotel-queue`        | Task queue name shared by worker and server                                   |
| `TEMPORAL_MAX_RETRIES`       | `20`                 | Max connection attempts before giving up                                      |
| `TEMPORAL_RETRY_DELAY_MS`    | `2000`               | Delay in ms between connection retries                                        |
| `API_URL`                    | `http://api:3000`    | Internal URL that Temporal activities use to call the mock supplier endpoints |

---

## API Reference

### `GET /health`

Returns the operational status of the service and both mock suppliers.

```bash
curl http://localhost:3000/health
```

```json
{
  "service": "healthy",
  "supplierA": "up",
  "supplierB": "up",
  "knownCities": ["bangalore", "delhi", "goa", "jaipur", "mumbai"],
  "config": {
    "redisTtlSeconds": 300,
    "temporalTaskQueue": "hotel-queue"
  }
}
```

---

### `GET /api/hotels`

Returns the deduplicated, best-priced hotel list for a city with optional price filtering.

| Parameter  | Required | Description                                                                              |
| ---------- | -------- | ---------------------------------------------------------------------------------------- |
| `city`     | ✅       | City name (case-insensitive). Supported: `delhi`, `mumbai`, `bangalore`, `goa`, `jaipur` |
| `minPrice` | ❌       | Only return hotels at or above this price (INR)                                          |
| `maxPrice` | ❌       | Only return hotels at or below this price (INR)                                          |

**Response** — a JSON array of hotel objects:

```json
[
  {
    "name": "Holtin Express",
    "price": 5900,
    "supplier": "Supplier B",
    "commissionPct": 20
  },
  {
    "name": "Bloom Suites",
    "price": 5800,
    "supplier": "Supplier A",
    "commissionPct": 20
  }
]
```

**Example requests:**

```bash
# All hotels in Delhi
curl "http://localhost:3000/api/hotels?city=delhi"

# Only hotels at ₹5,000 or above
curl "http://localhost:3000/api/hotels?city=delhi&minPrice=5000"

# Only hotels at ₹8,000 or below
curl "http://localhost:3000/api/hotels?city=delhi&maxPrice=8000"

# Price range filter
curl "http://localhost:3000/api/hotels?city=delhi&minPrice=5000&maxPrice=10000"

# Unknown city — returns empty array (result cached as sentinel for 60s)
curl "http://localhost:3000/api/hotels?city=moon"

# Missing city param — returns 400
curl "http://localhost:3000/api/hotels"

# Case-insensitive city name
curl "http://localhost:3000/api/hotels?city=DELHI"
```

**Status codes:**

| Code  | Meaning                                            |
| ----- | -------------------------------------------------- |
| `200` | Success — may be an empty array for unknown cities |
| `400` | Missing required `city` parameter                  |
| `500` | Internal error (Temporal or Redis unavailable)     |

---

### `GET /supplierA/hotels?city={city}` and `GET /supplierB/hotels?city={city}`

Internal mock supplier endpoints, also accessible directly for inspection or testing.

```bash
curl "http://localhost:3000/supplierA/hotels?city=delhi"
curl "http://localhost:3000/supplierB/hotels?city=mumbai"
```

Each returns a raw supplier array:

```json
[
  {
    "hotelId": "a-del-1",
    "name": "The Leela Palace",
    "price": 22000,
    "city": "delhi",
    "commissionPct": 12
  }
]
```

---

## How It Works

### Cache hit (fast path)

1. `GET /api/hotels?city=delhi` arrives
2. Server checks Redis for a sentinel key — not found
3. Server checks Redis for `hotels:delhi` sorted set — **exists**
4. `ZRANGEBYSCORE hotels:delhi <min> <max>` returns filtered results instantly
5. Response delivered in milliseconds, no Temporal workflow involved

### Cache miss (first request for a city)

1. No Redis data found for the city
2. Server starts a **Temporal workflow** (`hotelAggregationWorkflow`)
3. Workflow calls `fetchSupplierA` and `fetchSupplierB` **in parallel** via `Promise.all`
4. Each activity makes an HTTP call to the corresponding mock supplier endpoint
5. Results are merged — for each hotel name, the cheaper offer wins; single-supplier hotels are always included
6. `saveToRedis` writes all hotels to a sorted set keyed `hotels:<city>` with price as score, TTL = 5 minutes
7. Price filter is applied to the fresh results and returned to the client

### Unknown city path

1. First request: workflow runs, both suppliers return `[]`, a sentinel key is written with a 60s TTL
2. Any request within the next 60 seconds: sentinel check returns immediately with an empty array — no workflow is spawned

### Deduplication example (Delhi)

| Hotel            | Supplier A | Supplier B | Winner                       |
| ---------------- | ---------- | ---------- | ---------------------------- |
| The Leela Palace | ₹22,000    | ₹21,500    | **Supplier B** — cheaper     |
| Taj Mahal Hotel  | ₹18,500    | ₹19,000    | **Supplier A** — cheaper     |
| Radisson Blu     | ₹9,500     | ₹9,200     | **Supplier B** — cheaper     |
| Holtin Express   | ₹6,200     | ₹5,900     | **Supplier B** — cheaper     |
| Bloom Suites     | ₹5,800     | ₹6,100     | **Supplier A** — cheaper     |
| The Grand        | —          | ₹13,000    | **Supplier B** — only source |
| Metro Lodge      | ₹1,800     | —          | **Supplier A** — only source |

---

## Supported Cities & Data

Both suppliers cover 5 cities with 7–9 hotels each. Hotel names deliberately overlap between suppliers with different prices so deduplication is always meaningful at every city.

| City        | Hotels per supplier | Overlapping names                                                                                |
| ----------- | ------------------- | ------------------------------------------------------------------------------------------------ |
| `delhi`     | 9                   | The Leela Palace, Taj Mahal Hotel, Radisson Blu, Holtin Express, Bloom Suites, Capital O Premium |
| `mumbai`    | 8                   | Taj Lands End, The Trident, Radisson Blu, Bloom Suites, Holtin Express, City Budget Stay         |
| `bangalore` | 8                   | The Ritz-Carlton, ITC Gardenia, Radisson Blu, Bloom Suites, Holtin Express, Budget Nest          |
| `goa`       | 7                   | Park Hyatt Goa, Taj Exotica, Bloom Suites, Holtin Express, Shack & Stay                          |
| `jaipur`    | 7                   | Rambagh Palace, Taj Jai Mahal Palace, Radisson Blu, Bloom Suites, Holtin Express                 |

Price ranges span **₹1,600 – ₹35,000** per night, ensuring meaningful results at any `minPrice`/`maxPrice` combination.

---

## Postman Collection

Import `hotel-orchestrator_postman_collection.json` into Postman to run the full test suite (16 requests across 7 groups).

| Group              | What is tested                                                                                             |
| ------------------ | ---------------------------------------------------------------------------------------------------------- |
| Health             | Status 200, both suppliers up, `knownCities` present                                                       |
| Delhi              | 1st call (Temporal workflow), 2nd call (Redis cache), minPrice, maxPrice, price range, out-of-range filter |
| Mumbai             | All hotels, price range                                                                                    |
| Bangalore          | All hotels                                                                                                 |
| Goa                | All hotels, luxury filter (`minPrice=15000`)                                                               |
| Jaipur             | All hotels                                                                                                 |
| Error cases        | Unknown city (1st + 2nd call), missing `city` param (400), case-insensitive city name                      |
| Supplier endpoints | Supplier A direct call, Supplier B direct call, missing `city` on supplier (400)                           |

Every request includes **automatic test assertions** on status codes, response shape, price range correctness, and deduplication.

**Run via Newman (CLI runner):**

```bash
npm install -g newman
newman run hotel-orchestrator_postman_collection.json
```

**Simulating a supplier being down:**

To test supplier failure, stop the entire stack and restart with `API_URL` pointing to a host that refuses connections:

```bash
API_URL=http://nowhere:9999 docker compose up
```

The Temporal activity will retry 3 times before failing. The workflow error is surfaced to the client as a `500` with a descriptive message. You can observe the retry attempts in the Temporal UI at `http://localhost:8233`.

---

## Stopping the Stack

```bash
# Stop all containers — Redis and Postgres data preserved
docker compose down

# Stop all containers and wipe all volumes — fully clean state
docker compose down -v
```
