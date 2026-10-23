# 🚀 11-Core Autoscaling Node.js Backend (RabbitMQ + PM2 / IPC Dispatcher)

A high-performance implementation of dynamic autoscaling from **0 to 10 instances and back down to 0** using **RabbitMQ** and **Node.js IPC**, featuring an ultra-fast **Windows Task Manager style 11-Core CPU grid dashboard**.

---

## ⚡ Architecture: 11 Total Cores

```
                         [ Traffic Injector / cURL / Web Dashboard ]
                                             │
                       (Query Param: ?count=30&min=100&max=2000)
                                             ▼
                 ┌────────────────────────────────────────────────────────┐
                 │          RabbitMQ Message Broker (localhost:5672)      │
                 │                 Queue: 'my-tasks' (Durable)            │
                 └───────────────────────────┬────────────────────────────┘
                                             │ AMQP Consume (prefetch = 10)
                                             ▼
                               ┌───────────────────────────┐
                               │    Core #00: Manager      │
                               │  100% Asynchronous & Fast │
                               │  (HTTP, SSE, IPC Router)  │
                               └─────────────┬─────────────┘
                                             │ Scales 0 ➔ 10 Workers via IPC
                         ┌───────────────────┴───────────────────┐
                         ▼                                       ▼
            ┌─────────────────────────┐             ┌─────────────────────────┐
            │  Core #01: Worker #1    │    . . .    │  Core #10: Worker #10   │
            │ Pure Synchronous Freeze │             │ Pure Synchronous Freeze │
            │      (0.1s - 2.0s)      │             │      (0.1s - 2.0s)      │
            └────────────┬────────────┘             └────────────┬────────────┘
                         │ IPC { type: 'DONE' }                  │ IPC { type: 'DONE' }
                         ▼                                       ▼
            [ Manager ACKs RabbitMQ ➔ Scale-to-0 (All idle workers exit cleanly) ]
```

### 1. The Manager Core (Core #00 - 100% Asynchronous)
* Runs in a non-blocking, event-driven Node.js loop.
* Consumes messages from RabbitMQ using `channel.prefetch(10)`.
* **Zero task-stealing:** Routes tasks directly to available worker slots via Node.js IPC (`child.send({ type: 'TASK' })`).
* Asynchronously acknowledges (`channel.ack()`) messages back to RabbitMQ only when the worker signals completion.
* Manages automatic pool scaling (0 to 10 instances) and scale-to-zero when idle.
* Serves the real-time **11-Core Windows Task Manager Web Dashboard** on `http://localhost:4000` via SSE (Server-Sent Events) at 100ms intervals.

### 2. The Worker Cores (Cores #01–#10 - Strictly Synchronous Blocking)
* 10 isolated Node.js child processes / PM2 instances.
* Freezes its dedicated thread's event loop using a **pure synchronous blocking while loop** for the task duration (`100ms` to `2000ms`).
* **Zero external network overhead:** Does not connect to RabbitMQ directly; communicates purely with the Manager via lightweight IPC.
* **Scale-to-0:** When idle, workers terminate cleanly on Manager signal.

---

## 🛠️ Prerequisites & Setup

### 1. Start RabbitMQ (Docker)
```bash
docker compose up -d
```
*(Runs RabbitMQ 3 Management container on `localhost:5672` with Management Web UI on `localhost:15672`).*

### 2. Install Dependencies with pnpm
```bash
pnpm install
```

### 3. Start the Manager (11-Core Controller)
```bash
pnpm start
# OR
node manager.js
```
Open your browser to: **[http://localhost:4000](http://localhost:4000)**

---

## 📊 How to Trigger Traffic & Watch Autoscaling

### Option A: Via the Web Dashboard (One-Click)
Open **`http://localhost:4000`** in your browser. You can click:
- `+10 Jobs` (Quick scale test)
- `⚡ +30 Jobs (Standard Panic Burst)` (Watch 10 workers spin up instantly)
- `+100 Jobs` (Sustained load)
- `🔥 +1,000 Jobs` (1K spike)
- `🚀 +5,000 Jobs` (Heavy spike)
- **Continuous Traffic Toggle**: Simulates real production background load.
- **Duration inputs**: Configurable between `100ms` and `2000ms` (0.1s to 2.0s).

### Option B: Via Terminal CLI
In a second terminal window:
```bash
# Standard burst (30 jobs, randomized 0.1 to 2.0s duration)
pnpm run trigger:30

# Custom count and duration
node test-trigger.js 5 1000              # 5 jobs, each exactly 1000ms (1s)
node test-trigger.js 50 500              # 50 jobs, each exactly 500ms
node test-trigger.js 200 --min=100 --max=1000
```

### Option C: Via HTTP Query Parameters (cURL / Browser)
You can inject traffic directly using URL query parameters:
```bash
# Single request taking 1.5 seconds:
curl -X POST "http://localhost:4000/enqueue?duration=1500"

# Burst of 30 requests with random duration between 100ms and 2000ms:
curl -X POST "http://localhost:4000/enqueue?count=30&min=100&max=2000"
```
