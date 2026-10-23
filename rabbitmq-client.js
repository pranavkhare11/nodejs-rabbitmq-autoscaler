/**
 * rabbitmq-client.js - Shared RabbitMQ Configuration and Connection Helper
 */

import amqp from 'amqplib';

export const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';
export const QUEUE_NAME = 'my-tasks';
export const MAX_WORKERS = 10;

/**
 * Connect to RabbitMQ with retry mechanism
 */
export async function connectRabbitMQ(retries = 10, delay = 2000) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const conn = await amqp.connect(RABBITMQ_URL);
      const channel = await conn.createChannel();
      
      // Ensure the durable task queue exists
      await channel.assertQueue(QUEUE_NAME, {
        durable: true
      });

      return { conn, channel };
    } catch (err) {
      if (attempt === retries) {
        throw new Error(`Failed to connect to RabbitMQ after ${retries} attempts: ${err.message}`);
      }
      await new Promise(res => setTimeout(res, delay));
    }
  }
}

/**
 * Check current queue depth (unacknowledged + ready messages)
 */
export async function getQueueDepth(channel) {
  try {
    const q = await channel.checkQueue(QUEUE_NAME);
    return {
      messageCount: q.messageCount || 0,
      consumerCount: q.consumerCount || 0
    };
  } catch (err) {
    return { messageCount: 0, consumerCount: 0 };
  }
}
