/**
 * kafka-client.js - Shared Kafka Configuration and Lag Calculation Helper
 */

import { Kafka, logLevel } from 'kafkajs';

export const KAFKA_BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
export const TOPIC_NAME = 'my-tasks';
export const CONSUMER_GROUP = 'sync-workers-group';
export const NUM_PARTITIONS = 10;

export const kafka = new Kafka({
  clientId: 'kafka-pm2-autoscaler',
  brokers: [KAFKA_BROKER],
  logLevel: logLevel.NOTHING, // Completely silent - no JSON rebalance logs
  retry: {
    initialRetryTime: 300,
    retries: 5
  }
});

/**
 * Fetch Kafka lag accurately by comparing partition High Watermarks (latest offset)
 * with the consumer group committed offsets.
 */
export async function getTopicLag(admin) {
  try {
    const topicOffsets = await admin.fetchTopicOffsets(TOPIC_NAME);
    let groupOffsets = [];
    try {
      groupOffsets = await admin.fetchOffsets({
        groupId: CONSUMER_GROUP,
        topics: [TOPIC_NAME]
      });
    } catch (_) {
      // Group coordinator still electing or group empty
    }

    let totalLag = 0;
    const partitionLags = {};

    const groupPartitions = groupOffsets[0]?.partitions || [];

    for (const tOffset of topicOffsets) {
      const partId = tOffset.partition;
      const highOffset = BigInt(tOffset.offset || tOffset.high || '0');
      
      const gPart = groupPartitions.find(p => p.partition === partId);
      let committedOffset = BigInt(0);

      if (gPart && gPart.offset !== '-1') {
        committedOffset = BigInt(gPart.offset);
      } else {
        // If consumer has never committed on this partition, uncommitted lag is from low to high
        committedOffset = BigInt(tOffset.low || '0');
      }

      const lag = Number(highOffset - committedOffset);
      const positiveLag = lag > 0 ? lag : 0;
      partitionLags[partId] = positiveLag;
      totalLag += positiveLag;
    }

    return { totalLag, partitionLags };
  } catch (err) {
    // Return 0 if topic or group not yet established
    return { totalLag: 0, partitionLags: {} };
  }
}
