/**
 * manager.js - The 11-Core Ephemeral Worker Autoscaler & RabbitMQ Dispatcher
 * 
 * Architecture:
 * 1. 1 ASYNCHRONOUS MANAGER CORE (Core #00):
 *    - 100% Non-blocking, event-driven Node.js event loop.
 *    - Connected to RabbitMQ (amqp://localhost:5672) queue 'my-tasks'.
 *    - Consumes tasks asynchronously with prefetch(10).
 *    - Immediately dispatches tasks by spawning dedicated single-task workers into free slots.
 *    - As soon as a worker finishes its task and exits, the Manager IMMEDIATELY pushes the next task into that slot!
 *    - When queue is empty, workers exit naturally, leaving the system at scale 0.
 * 
 * 2. 10 SYNCHRONOUS WORKER CORES (Cores #01 - #10):
 *    - 10 isolated Node.js processes (capped at 10 concurrent).
 *    - Dedicated to exactly ONE task: pure synchronous lock (100ms - 2000ms).
 *    - Exits immediately (process.exit(0)) once finished. Never waits idle.
 */

import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { fork } from 'node:child_process';
import pm2 from 'pm2';
import { connectRabbitMQ, getQueueDepth, QUEUE_NAME, MAX_WORKERS } from './rabbitmq-client.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configuration
const PORT = parseInt(process.env.PORT || '4000', 10);
const WORKER_APP_NAME = 'rabbit-sync-worker';
const USE_PM2 = process.argv.includes('--pm2') || process.env.USE_PM2 === 'true';

// ANSI Colors
const C = {
  reset: '\x1b[0m',
  bold: '\x1b[1m',
  dim: '\x1b[2m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  magenta: '\x1b[35m',
  cyan: '\x1b[36m',
  white: '\x1b[37m',
  bgRed: '\x1b[41m',
  bgGreen: '\x1b[42m',
  bgBlue: '\x1b[44m',
};

function termLog(msg) {
  const ts = new Date().toISOString().substring(11, 23);
  console.log(`${C.dim}[${ts}]${C.reset} ${C.blue}${C.bold}[Manager Core #0]${C.reset} ${msg}`);
}

// ---------------------------------------------------------------------------
// 1. STATE & 10 LOGICAL WORKER SLOTS
// ---------------------------------------------------------------------------
let rabbitConn = null;
let rabbitChannel = null;

let queueDepth = 0;
let totalProcessed = 0;
let recentProcessedCount = 0;
let throughput = 0;
let avgDuration = 1000;
const durationHistory = [];
let isSystemIdle = true;

// Queue of waiting tasks
const pendingDispatchQueue = [];

// 10 Logical Slots for the Worker Cores
const workerSlots = Array.from({ length: MAX_WORKERS }, (_, i) => ({
  slot: i + 1,
  active: false,
  processRef: null,
  pm2Id: null,
  pid: null,
  jobId: null,
  duration: 0,
  startedAt: 0,
  completedJobs: 0,
  rabbitMsgRef: null
}));

// Throughput calculator (every 1 second)
setInterval(() => {
  throughput = recentProcessedCount;
  recentProcessedCount = 0;
}, 1000);

// SSE clients for real-time web dashboard
const sseClients = new Set();

function broadcastEvent(type, text) {
  const payload = JSON.stringify({
    type,
    text,
    time: new Date().toISOString().substring(11, 23)
  });
  for (const res of sseClients) {
    try {
      res.write(`event: log\ndata: ${payload}\n\n`);
    } catch (_) {}
  }
}

function getActiveWorkerCount() {
  return workerSlots.filter(s => s.active).length;
}

function getFreeSlot() {
  return workerSlots.find(s => !s.active);
}

// ---------------------------------------------------------------------------
// 2. DISPATCHER & EPHEMERAL WORKER SPAWNER
// ---------------------------------------------------------------------------

/**
 * Dispatches waiting tasks to any free slots (up to 10 max concurrent).
 * As soon as a worker exits, this function is triggered immediately to push the next task.
 */
function dispatchWaitingTasks() {
  while (pendingDispatchQueue.length > 0) {
    const freeSlot = getFreeSlot();
    if (!freeSlot) break; // All 10 worker slots are currently executing a task

    const nextTaskItem = pendingDispatchQueue.shift();
    const { taskData, rabbitMsg } = nextTaskItem;
    const slotNum = freeSlot.slot;

    isSystemIdle = false;
    freeSlot.active = true;
    freeSlot.jobId = taskData.id;
    freeSlot.duration = taskData.duration;
    freeSlot.startedAt = Date.now();
    freeSlot.rabbitMsgRef = rabbitMsg;

    if (USE_PM2) {
      // PM2 Mode: start 1 instance for this task
      pm2.start({
        script: 'worker.js',
        name: `${WORKER_APP_NAME}-${slotNum}`,
        instances: 1,
        exec_mode: 'fork',
        autorestart: false,
        env: {
          WORKER_SLOT: slotNum.toString(),
          TASK_ID: taskData.id,
          TASK_DURATION: taskData.duration.toString()
        },
        args: [slotNum.toString()]
      }, (err, apps) => {
        if (err) {
          termLog(`${C.red}PM2 start error on slot ${slotNum}: ${err.message}${C.reset}`);
          freeSlot.active = false;
          return;
        }
        const pmId = apps[0]?.pm2_env?.pm_id;
        freeSlot.pm2Id = pmId;
        freeSlot.pid = apps[0]?.pid || `pm2-${pmId}`;
      });
    } else {
      // Standard Node.js Fork Mode (Instant, sub-millisecond start)
      const child = fork(path.join(__dirname, 'worker.js'), [slotNum.toString()], {
        env: {
          ...process.env,
          WORKER_SLOT: slotNum.toString(),
          TASK_ID: taskData.id,
          TASK_DURATION: taskData.duration.toString()
        },
        execArgv: ['--no-warnings'],
        stdio: ['inherit', 'pipe', 'pipe', 'ipc']
      });

      child.stdout.on('data', data => process.stdout.write(data));
      child.stderr.on('data', data => process.stderr.write(data));

      freeSlot.processRef = child;
      freeSlot.pid = child.pid;

      child.on('message', (packet) => {
        const msg = packet?.data || packet;
        if (!msg) return;

        if (msg.type === 'FREEZING') {
          broadcastEvent('freeze', `Worker Core #${slotNum} [PID ${freeSlot.pid}] locked in pure synchronous freeze for ${msg.duration}ms on ${msg.id}`);
        } else if (msg.type === 'DONE') {
          // Acknowledge to RabbitMQ
          if (freeSlot.rabbitMsgRef && rabbitChannel) {
            try {
              rabbitChannel.ack(freeSlot.rabbitMsgRef);
            } catch (err) {
              termLog(`${C.red}RabbitMQ ACK failed: ${err.message}${C.reset}`);
            }
            freeSlot.rabbitMsgRef = null;
          }

          freeSlot.completedJobs++;
          totalProcessed++;
          recentProcessedCount++;

          if (msg.duration) {
            durationHistory.push(msg.duration);
            if (durationHistory.length > 50) durationHistory.shift();
            avgDuration = Math.round(durationHistory.reduce((a, b) => a + b, 0) / durationHistory.length);
          }

          broadcastEvent('commit', `Worker Core #${slotNum} completed ${msg.id}. RabbitMQ ACK sent.`);
        }
      });

      // WHEN WORKER EXITS: Immediately free slot and push next task!
      child.on('exit', () => {
        freeSlot.active = false;
        freeSlot.processRef = null;
        freeSlot.pid = null;
        freeSlot.jobId = null;
        freeSlot.duration = 0;
        freeSlot.rabbitMsgRef = null;

        const remaining = getActiveWorkerCount();
        broadcastEvent('scale_down', `Worker Core #${slotNum} exited. Active: ${remaining}/${MAX_WORKERS}`);

        // AS SOON AS IT EXITS: Immediately push next task if available!
        dispatchWaitingTasks();

        if (remaining === 0 && pendingDispatchQueue.length === 0 && !isSystemIdle) {
          isSystemIdle = true;
          termLog(`${C.green}${C.bold}🟢 All tasks finished. All workers scaled to 0 (Idle).${C.reset}`);
          broadcastEvent('scale_down', `All workers scaled down to 0. System is idling.`);
        }
      });
    }

    broadcastEvent('scale_up', `Started Worker Core #${slotNum} [PID ${freeSlot.pid}] for ${taskData.id}. Active: ${getActiveWorkerCount()}/${MAX_WORKERS}`);
  }
}

// Periodic queue depth synchronizer
setInterval(async () => {
  if (rabbitChannel) {
    try {
      const q = await getQueueDepth(rabbitChannel);
      queueDepth = q.messageCount + pendingDispatchQueue.length;
    } catch (_) {}
  }

  // PM2 cleanup for stopped instances
  if (USE_PM2) {
    pm2.list((err, list) => {
      if (err || !list) return;
      for (const app of list) {
        if (app.name.startsWith(WORKER_APP_NAME) && app.pm2_env.status === 'stopped') {
          pm2.delete(app.pm2_env.pm_id, () => {});
          const slotNum = parseInt(app.name.replace(`${WORKER_APP_NAME}-`, ''), 10);
          const slot = workerSlots.find(s => s.slot === slotNum);
          if (slot) {
            slot.active = false;
            slot.pm2Id = null;
            slot.pid = null;
            slot.jobId = null;
            slot.duration = 0;
            // As soon as PM2 exits, push next task!
            dispatchWaitingTasks();
          }
        }
      }
    });
  }
}, 500);

// ---------------------------------------------------------------------------
// 3. RABBITMQ CONSUMER SETUP (ASYNCHRONOUS EVENT LOOP)
// ---------------------------------------------------------------------------
async function initRabbitMQ() {
  termLog(`${C.yellow}Connecting to RabbitMQ cluster at amqp://localhost:5672...${C.reset}`);

  try {
    const { conn, channel } = await connectRabbitMQ(15, 2000);
    rabbitConn = conn;
    rabbitChannel = channel;

    // Prefetch up to 10 unacknowledged messages across the 10 slots
    await channel.prefetch(MAX_WORKERS);

    termLog(`${C.green}${C.bold}✓ RabbitMQ connected successfully! Queue '${QUEUE_NAME}' ready with prefetch=${MAX_WORKERS}.${C.reset}`);

    // Asynchronous non-blocking message consumer
    await channel.consume(QUEUE_NAME, (msg) => {
      if (!msg) return;

      let taskData = {};
      try {
        taskData = JSON.parse(msg.content.toString());
      } catch (_) {
        taskData = { id: `task-${Date.now()}`, duration: 1000 };
      }

      // Add to internal dispatch queue
      pendingDispatchQueue.push({
        taskData,
        rabbitMsg: msg
      });

      // Immediately dispatch or push to free worker slots
      dispatchWaitingTasks();
    }, {
      noAck: false // Manual ACK on task completion
    });

  } catch (err) {
    termLog(`${C.red}RabbitMQ initialization error: ${err.message}${C.reset}`);
    termLog(`${C.yellow}Will retry RabbitMQ connection in 3 seconds...${C.reset}`);
    setTimeout(initRabbitMQ, 3000);
  }
}

// ---------------------------------------------------------------------------
// 4. HTTP SERVER & DASHBOARD API
// ---------------------------------------------------------------------------
const server = http.createServer(async (req, res) => {
  const urlObj = new URL(req.url, `http://${req.headers.host}`);
  const pathname = urlObj.pathname;
  const searchParams = urlObj.searchParams;

  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  // 1. WEB DASHBOARD UI
  if (pathname === '/' || pathname === '/index.html') {
    const htmlPath = path.join(__dirname, 'public', 'index.html');
    if (fs.existsSync(htmlPath)) {
      res.writeHead(200, { 'Content-Type': 'text/html' });
      fs.createReadStream(htmlPath).pipe(res);
      return;
    }
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('Manager running. Dashboard index.html not found in public folder.');
    return;
  }

  // 2. ENQUEUE TRAFFIC TO RABBITMQ
  if (pathname === '/enqueue' || pathname === '/api/enqueue') {
    let bodyData = '';
    req.on('data', chunk => { bodyData += chunk; });
    req.on('end', async () => {
      let body = {};
      try { if (bodyData) body = JSON.parse(bodyData); } catch (_) {}

      const count = parseInt(searchParams.get('count') || body.count || '1', 10);
      const minDuration = parseInt(searchParams.get('min') || body.min || '100', 10);
      const maxDuration = parseInt(searchParams.get('max') || body.max || '2000', 10);
      const rawDuration = searchParams.get('t') || searchParams.get('duration') || body.t || body.duration;
      const explicitDuration = rawDuration ? parseInt(rawDuration, 10) : null;

      if (!rabbitChannel) {
        res.writeHead(503, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'RabbitMQ channel not ready' }));
        return;
      }

      const timestamp = Date.now();
      for (let i = 0; i < count; i++) {
        let dur = explicitDuration;
        if (!dur) {
          dur = Math.floor(Math.random() * (maxDuration - minDuration + 1)) + minDuration;
        }

        const jobId = `job-${String(i + 1).padStart(5, '0')}-${timestamp}`;
        const taskPayload = {
          id: jobId,
          duration: dur,
          createdAt: Date.now()
        };

        rabbitChannel.sendToQueue(
          QUEUE_NAME,
          Buffer.from(JSON.stringify(taskPayload)),
          { persistent: true }
        );
      }

      const q = await getQueueDepth(rabbitChannel);
      queueDepth = q.messageCount + pendingDispatchQueue.length;

      termLog(`${C.magenta}📥 Injected ${C.bold}${count}${C.reset} tasks into RabbitMQ queue '${QUEUE_NAME}'. Backlog: ${C.red}${C.bold}${queueDepth}${C.reset}`);
      broadcastEvent('flood', `Injected ${count} tasks into RabbitMQ. Queue backlog: ${queueDepth}`);

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        success: true,
        enqueued: count,
        queueDepth: queueDepth,
        activeWorkers: getActiveWorkerCount(),
        maxWorkers: MAX_WORKERS,
        message: `Published ${count} jobs to RabbitMQ queue '${QUEUE_NAME}'`
      }));
    });
    return;
  }

  // 3. PURGE RABBITMQ QUEUE
  if (pathname === '/api/clear' && req.method === 'POST') {
    if (rabbitChannel) {
      try {
        await rabbitChannel.purgeQueue(QUEUE_NAME);
        pendingDispatchQueue.length = 0;
        queueDepth = 0;
      } catch (_) {}
    }
    termLog(`${C.yellow}🧹 Queue purged by operator.${C.reset}`);
    broadcastEvent('scale_down', `RabbitMQ queue purged. Backlog cleared.`);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ success: true, message: 'Queue purged' }));
    return;
  }

  // 4. API LAG / QUEUE METRICS
  if (pathname === '/api/lag' || pathname === '/api/queue') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      lag: queueDepth,
      busyWorkers: getActiveWorkerCount(),
      activeWorkers: getActiveWorkerCount()
    }));
    return;
  }

  // 5. SERVER-SENT EVENTS (SSE) STREAM FOR 100ms HIGH FREQUENCY DASHBOARD
  if (pathname === '/api/stream') {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'Access-Control-Allow-Origin': '*'
    });
    res.write('\n');

    sseClients.add(res);

    const initialSnapshot = JSON.stringify({
      managerPid: process.pid,
      mode: USE_PM2 ? 'PM2' : 'Fork',
      lag: queueDepth,
      activeWorkers: getActiveWorkerCount(),
      busyWorkers: getActiveWorkerCount(),
      totalProcessed,
      throughput,
      avgDuration,
      slots: workerSlots.map(s => ({
        slot: s.slot,
        active: s.active,
        isBusy: s.active,
        pid: s.pid,
        jobId: s.jobId,
        duration: s.duration,
        startedAt: s.startedAt,
        completedJobs: s.completedJobs
      }))
    });
    res.write(`data: ${initialSnapshot}\n\n`);

    req.on('close', () => {
      sseClients.delete(res);
    });
    return;
  }

  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: 'Endpoint not found' }));
});

// SSE Snapshot interval (100ms - ultra responsive Task Manager UI)
setInterval(() => {
  if (sseClients.size === 0) return;

  const snapshot = JSON.stringify({
    managerPid: process.pid,
    mode: USE_PM2 ? 'PM2' : 'Fork',
    lag: queueDepth,
    activeWorkers: getActiveWorkerCount(),
    busyWorkers: getActiveWorkerCount(),
    totalProcessed,
    throughput,
    avgDuration,
    slots: workerSlots.map(s => ({
      slot: s.slot,
      active: s.active,
      isBusy: s.active,
      pid: s.pid,
      jobId: s.jobId,
      duration: s.duration,
      startedAt: s.startedAt,
      completedJobs: s.completedJobs
    }))
  });

  for (const client of sseClients) {
    try {
      client.write(`data: ${snapshot}\n\n`);
    } catch (_) {}
  }
}, 100);

// Graceful shutdown
function shutdownAll() {
  termLog(`${C.yellow}Shutting down manager and worker processes...${C.reset}`);
  for (const slot of workerSlots) {
    if (slot.processRef) {
      try { slot.processRef.kill(); } catch (_) {}
    }
  }
  setTimeout(() => {
    try { if (rabbitConn) rabbitConn.close(); } catch (_) {}
    process.exit(0);
  }, 500);
}

process.on('SIGINT', shutdownAll);
process.on('SIGTERM', shutdownAll);

// Start HTTP Server and RabbitMQ Connection
server.listen(PORT, () => {
  console.log(`
${C.cyan}${C.bold}======================================================================${C.reset}
${C.green}${C.bold}   11-CORE EPHEMERAL AUTOSCALING BACKEND (RABBITMQ + PM2 / FORK)    ${C.reset}
${C.cyan}${C.bold}======================================================================${C.reset}

  ${C.bold}Manager Core #0:${C.reset}     100% Asynchronous Event Loop (HTTP + AMQP Dispatcher)
  ${C.bold}Worker Cores 1-10:${C.reset}   Ephemeral Single-Task Synchronous Cores (0 to 10 Scale)
  ${C.bold}Task Lifecycle:${C.reset}      1 Task ➔ Synchronous Lock ➔ Process Exits ➔ Next Task Pushed
  ${C.bold}RabbitMQ Broker:${C.reset}     amqp://guest:guest@localhost:5672 (Queue: '${QUEUE_NAME}')
  ${C.bold}Web Dashboard:${C.reset}       ${C.cyan}${C.bold}http://localhost:${PORT}${C.reset}
  ${C.bold}RabbitMQ Admin UI:${C.reset}   http://localhost:15672 (guest/guest)
  ${C.bold}Mode:${C.reset}                ${USE_PM2 ? 'PM2 Supervised' : 'Native Node.js Child Process Fork'}
======================================================================
  `);

  initRabbitMQ();
});
