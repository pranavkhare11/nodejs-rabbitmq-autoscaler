/**
 * worker.js - The Single-Task Synchronous Blocking Worker
 * 
 * Rules:
 * 1. Dedicated execution core spawned by manager.js for exactly ONE task.
 * 2. Freezes this thread's event loop for the requested duration (100ms - 2000ms).
 * 3. Immediately exits (process.exit(0)) once the task finishes.
 * 4. NEVER waits idle for a new task.
 */

const WORKER_SLOT = process.argv[2] || process.env.WORKER_SLOT || '1';
const PID = process.pid;

// ANSI Colors for terminal logs
const C = {
  reset: '\x1b[0m',
  bold: '\x1b[1m',
  dim: '\x1b[2m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  cyan: '\x1b[36m',
  magenta: '\x1b[35m',
};

function log(msg) {
  const ts = new Date().toISOString().substring(11, 23);
  console.log(`${C.dim}[${ts}]${C.reset} ${C.cyan}${C.bold}[Worker Core #${WORKER_SLOT} | PID ${PID}]${C.reset} ${msg}`);
}

/**
 * Pure synchronous blocking function.
 * Freezes Node's single-threaded event loop for the specified milliseconds.
 */
function syncFreeze(ms) {
  const start = Date.now();
  while (Date.now() - start < ms) {
    // Pure synchronous busy loop - event loop is completely frozen
  }
}

// Send IPC message safely to parent (manager.js)
function sendIPC(msg, callback) {
  if (typeof process.send === 'function') {
    process.send(msg, callback);
  } else if (callback) {
    callback();
  }
}

// Execute the task and immediately terminate
function runTask(id, duration) {
  const shortId = (id || 'task').replace(/-\d+$/, '').replace(/^job-/, '#');
  log(`${C.magenta}⏳ Executing ${shortId} (${(duration / 1000).toFixed(2)}s synchronous lock)...${C.reset}`);

  // Notify manager that event loop is entering synchronous freeze
  sendIPC({
    type: 'FREEZING',
    slot: WORKER_SLOT,
    pid: PID,
    id: id,
    duration: duration,
    startedAt: Date.now()
  });

  // PURE SYNCHRONOUS BLOCK: FREEZE THE NODE.JS EVENT LOOP
  syncFreeze(duration);

  log(`${C.green}✅ Finished ${shortId}. Exiting core immediately.${C.reset}`);

  // Notify manager that execution completed and exit cleanly
  sendIPC({
    type: 'DONE',
    slot: WORKER_SLOT,
    pid: PID,
    id: id,
    duration: duration
  }, () => {
    // Worker does not wait for new task: exits immediately!
    process.exit(0);
  });
}

// Check if task was provided via environment variables at spawn time
if (process.env.TASK_ID && process.env.TASK_DURATION) {
  const dur = parseInt(process.env.TASK_DURATION, 10) || 1000;
  runTask(process.env.TASK_ID, dur);
} else {
  // Otherwise listen for task message via IPC
  process.on('message', (packet) => {
    const msg = packet?.data || packet;
    if (!msg) return;

    if (msg.type === 'TASK') {
      runTask(msg.id, msg.duration || 1000);
    } else if (msg.type === 'SHUTDOWN') {
      process.exit(0);
    }
  });
}

// Clean termination handlers
process.on('SIGINT', () => process.exit(0));
process.on('SIGTERM', () => process.exit(0));
