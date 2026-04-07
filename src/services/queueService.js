import { Queue, Worker } from 'bullmq';
import Redis from 'ioredis';

// Redis connections (BullMQ requires separate connections for Queue and Worker)
let queueConnection = null;
let workerConnection = null;
let documentQueue = null;
let useInMemoryQueue = false;

// In-memory queue fallback
const inMemoryJobs = [];
const jobHandlers = new Map();

function createRedisConnection(name) {
  const redisUrl = process.env.UPSTASH_REDIS_URL;
  
  if (!redisUrl) {
    return null;
  }

  const conn = new Redis(redisUrl, {
    maxRetriesPerRequest: null,
    enableReadyCheck: false,
    tls: redisUrl.startsWith('rediss://') ? {} : undefined,
    connectTimeout: 30000,
    keepAlive: 30000,
    family: 4,
    retryStrategy: (times) => {
      if (times > 5) {
        console.error(`[${name}] Redis connection failed after 5 retries`);
        return null;
      }
      const delay = Math.min(times * 1000, 5000);
      console.log(`[${name}] Redis retry ${times} in ${delay}ms`);
      return delay;
    }
  });

  conn.on('error', (err) => {
    console.error(`[${name}] Redis error:`, err.message);
  });

  conn.on('connect', () => {
    console.log(`[${name}] Redis connected`);
  });

  return conn;
}

export async function initializeQueue() {
  const redisUrl = process.env.UPSTASH_REDIS_URL;
  
  if (!redisUrl) {
    console.warn('⚠️  UPSTASH_REDIS_URL not set - using in-memory queue');
    useInMemoryQueue = true;
    return { useInMemory: true };
  }

  try {
    // Create separate connections for queue and worker
    queueConnection = createRedisConnection('Queue');
    workerConnection = createRedisConnection('Worker');

    // Wait for queue connection
    await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('Queue connection timeout')), 15000);
      queueConnection.once('ready', () => {
        clearTimeout(timeout);
        resolve();
      });
      queueConnection.once('error', (err) => {
        clearTimeout(timeout);
        reject(err);
      });
    });

    console.log('✓ Redis queue connection ready');

    // Create the queue
    documentQueue = new Queue('document-processing', { 
      connection: queueConnection,
      defaultJobOptions: {
        removeOnComplete: 100,
        removeOnFail: 50,
        attempts: 2,
        backoff: {
          type: 'exponential',
          delay: 5000
        }
      }
    });

    return { 
      useInMemory: false, 
      queueConnection, 
      workerConnection, 
      documentQueue 
    };
  } catch (err) {
    console.warn('⚠️  Redis connection failed:', err.message);
    console.warn('⚠️  Falling back to in-memory queue');
    useInMemoryQueue = true;
    return { useInMemory: true };
  }
}

// Export function to get worker connection
export function getWorkerConnection() {
  return workerConnection;
}

export function isUsingInMemoryQueue() {
  return useInMemoryQueue;
}

// Queue wrapper that works with both Redis and in-memory
export const queueManager = {
  async add(name, data, options = {}) {
    if (useInMemoryQueue || !documentQueue) {
      const job = { 
        id: options.jobId || Date.now().toString(), 
        name, 
        data,
        updateProgress: async () => {} // Mock for in-memory
      };
      inMemoryJobs.push(job);
      // Process immediately in-memory
      setTimeout(() => processInMemoryJob(job), 100);
      return job;
    }
    return documentQueue.add(name, data, options);
  }
};

async function processInMemoryJob(job) {
  const handler = jobHandlers.get('document-processing');
  if (handler) {
    try {
      await handler(job);
    } catch (err) {
      console.error('Job failed:', err);
    }
  }
}

export function registerJobHandler(queueName, handler) {
  jobHandlers.set(queueName, handler);
}

export { queueConnection as redisConnection, documentQueue };

// Job status store (in-memory for SSE, backed by Redis for persistence)
const jobStatusMap = new Map();

export function getJobStatus(jobId) {
  return jobStatusMap.get(jobId);
}

export function setJobStatus(jobId, status) {
  jobStatusMap.set(jobId, status);
}

export function updateJobStatus(jobId, updates) {
  const current = jobStatusMap.get(jobId) || {};
  const updated = { ...current, ...updates, updatedAt: new Date().toISOString() };
  jobStatusMap.set(jobId, updated);
  return updated;
}

// SSE clients management
const sseClients = new Map();

export function addSSEClient(jobId, res) {
  if (!sseClients.has(jobId)) {
    sseClients.set(jobId, new Set());
  }
  sseClients.get(jobId).add(res);
}

export function removeSSEClient(jobId, res) {
  if (sseClients.has(jobId)) {
    sseClients.get(jobId).delete(res);
    if (sseClients.get(jobId).size === 0) {
      sseClients.delete(jobId);
    }
  }
}

export function broadcastToJob(jobId, event, data) {
  if (sseClients.has(jobId)) {
    const message = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
    sseClients.get(jobId).forEach(client => {
      try {
        client.write(message);
      } catch (err) {
        console.error('SSE write error:', err);
      }
    });
  }
}
