/**
 * test-trigger.js - RabbitMQ Load Generator & Parallel HTTP Spiker
 * 
 * Injects messages directly into RabbitMQ or fires distinct parallel HTTP calls
 * where each request carries its own individual timer (?t=150 ... 2000ms).
 * 
 * Usage:
 *   node test-trigger.js 50                          (50 jobs with random 150-2000ms duration)
 *   node test-trigger.js 50 --http                   (50 distinct parallel HTTP requests with ?t=...)
 *   node test-trigger.js 100 --http --min=150 --max=2000
 *   node test-trigger.js 50 150                      (50 jobs, each fixed at 150ms)
 */

import { connectRabbitMQ, QUEUE_NAME, getQueueDepth } from './rabbitmq-client.js';

const MANAGER_HOST = process.env.MANAGER_HOST || '127.0.0.1';
const MANAGER_PORT = parseInt(process.env.MANAGER_PORT || '4000', 10);
const BASE_URL = `http://${MANAGER_HOST}:${MANAGER_PORT}`;

// CLI argument parsing
const args = process.argv.slice(2);
let count = 50;
let minDuration = 150;   // 150ms
let maxDuration = 2000;  // 2000ms
let fixedDuration = null;
const useHttp = args.includes('--http');

for (let i = 0; i < args.length; i++) {
  const arg = args[i];
  if (!isNaN(parseInt(arg, 10)) && i === 0) {
    count = parseInt(arg, 10);
  } else if (!isNaN(parseInt(arg, 10)) && i === 1) {
    fixedDuration = parseInt(arg, 10);
  } else if (arg.startsWith('--min=')) {
    minDuration = parseInt(arg.split('=')[1], 10);
  } else if (arg.startsWith('--max=')) {
    maxDuration = parseInt(arg.split('=')[1], 10);
  } else if (arg.startsWith('--duration=')) {
    fixedDuration = parseInt(arg.split('=')[1], 10);
  } else if (arg.startsWith('--t=')) {
    fixedDuration = parseInt(arg.split('=')[1], 10);
  }
}

// ANSI Colors
const C = {
  reset: '\x1b[0m',
  bold: '\x1b[1m',
  dim: '\x1b[2m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  cyan: '\x1b[36m',
  magenta: '\x1b[35m',
  white: '\x1b[37m',
  bgRed: '\x1b[41m',
  bgBlue: '\x1b[44m',
};

async function runTrafficFlood() {
  console.log(`\n${C.bgRed}${C.white}${C.bold}  TRAFFIC GENERATOR: PARALLEL REQUESTS FLOOD  ${C.reset}\n`);
  console.log(`${C.bold}Mode:${C.reset}            ${useHttp ? 'Separate Parallel HTTP Calls (fetch)' : 'Direct AMQP Pipeline'}`);
  console.log(`${C.bold}Target:${C.reset}          ${useHttp ? `${BASE_URL}/enqueue?t=...` : `RabbitMQ Queue '${QUEUE_NAME}'`}`);
  console.log(`${C.bold}Total Requests:${C.reset}  ${C.yellow}${C.bold}${count.toLocaleString()}${C.reset} distinct requests`);

  if (fixedDuration) {
    console.log(`${C.bold}Each Timer (?t):${C.reset} ${C.cyan}${fixedDuration}ms Fixed${C.reset}`);
  } else {
    console.log(`${C.bold}Each Timer (?t):${C.reset} ${C.cyan}Randomized ${minDuration}ms to ${maxDuration}ms each${C.reset}`);
  }

  const startTime = Date.now();

  if (useHttp) {
    // Mode A: Fire N distinct individual HTTP requests simultaneously in parallel!
    console.log(`\n${C.cyan}Dispatching ${count} independent HTTP requests concurrently...${C.reset}`);
    const requests = [];

    for (let i = 0; i < count; i++) {
      const dur = fixedDuration || (Math.floor(Math.random() * (maxDuration - minDuration + 1)) + minDuration);
      requests.push(
        fetch(`${BASE_URL}/enqueue?t=${dur}`, { method: 'POST' })
          .then(res => res.json())
          .catch(err => ({ error: err.message }))
      );
    }

    const results = await Promise.all(requests);
    const successful = results.filter(r => r.success).length;
    const elapsed = Date.now() - startTime;

    console.log(`\n${C.green}${C.bold}✓ All ${successful}/${count} Individual HTTP Requests Fired in ${elapsed}ms!${C.reset}`);
    console.log(`------------------------------------------------------------`);
    console.log(` ${C.bold}Architecture:${C.reset}        1 Async Manager Core + 10 Sync Worker Cores`);
    console.log(` ${C.bold}Pipeline:${C.reset}            Ephemeral workers process & exit, next tasks pushed instantly`);
    console.log(`------------------------------------------------------------`);
  } else {
    // Mode B: Direct AMQP messages into RabbitMQ queue
    try {
      const { conn, channel } = await connectRabbitMQ(5, 1000);
      const timestamp = Date.now();

      for (let i = 0; i < count; i++) {
        const dur = fixedDuration || (Math.floor(Math.random() * (maxDuration - minDuration + 1)) + minDuration);
        const jobId = `job-${String(i + 1).padStart(5, '0')}-${timestamp}`;
        const payload = {
          id: jobId,
          duration: dur,
          createdAt: Date.now()
        };

        channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify(payload)), {
          persistent: true
        });
      }

      const elapsed = Date.now() - startTime;
      const { messageCount } = await getQueueDepth(channel);

      console.log(`\n${C.green}${C.bold}✓ ${count} AMQP Tasks Successfully Published in ${elapsed}ms!${C.reset}`);
      console.log(`------------------------------------------------------------`);
      console.log(` ${C.bold}Queue Depth:${C.reset}         ${C.red}${C.bold}${messageCount} messages ready${C.reset}`);
      console.log(` ${C.bold}Architecture:${C.reset}        1 Async Manager Core + 10 Sync Worker Cores`);
      console.log(`------------------------------------------------------------`);

      await channel.close();
      await conn.close();
    } catch (err) {
      console.error(`\n${C.red}${C.bold}Error publishing to RabbitMQ:${C.reset} ${err.message}`);
      console.error(`${C.yellow}Ensure RabbitMQ is running (docker compose up -d)${C.reset}\n`);
      process.exit(1);
    }
  }

  console.log(`\n${C.bold}Watch the Cores in Real Time:${C.reset}`);
  console.log(` 1. In your ${C.cyan}manager.js terminal${C.reset}`);
  console.log(` 2. In your ${C.cyan}browser dashboard${C.reset}: ${C.bold}http://localhost:${MANAGER_PORT}${C.reset}\n`);
}

runTrafficFlood();
