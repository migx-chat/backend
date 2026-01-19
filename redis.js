const { createClient } = require('redis');

let redisUrl;

if (process.env.REDIS_URL) {
  redisUrl = process.env.REDIS_URL;
} else if (process.env.REDIS_HOST && process.env.REDIS_PORT && process.env.REDIS_PASSWORD) {
  redisUrl = `redis://default:${process.env.REDIS_PASSWORD}@${process.env.REDIS_HOST}:${process.env.REDIS_PORT}`;
} else {
  console.error('Redis configuration missing!');
  process.exit(1);
}

const client = createClient({
  url: redisUrl,
  socket: {
    reconnectStrategy: (retries) => {
      if (retries > 20) {
        console.error('Redis unreachable after 20 retries');
        return new Error('Redis unreachable');
      }
      return Math.min(retries * 50, 500);
    }
  }
});

client.on('error', (err) => {
  console.error('Redis Error:', err.message);
});

client.on('connect', () => {
  console.log('Connecting to Redis Cloud...');
});

client.on('ready', () => {
  console.log('Redis Cloud connected and ready');
});

client.on('reconnecting', () => {
  console.log('Redis reconnecting...');
});

const connectRedis = async () => {
  try {
    await client.connect();
    return client;
  } catch (error) {
    console.error('Failed to connect to Redis:', error.message);
    throw error;
  }
};

const getRedisClient = () => {
  return client;
};

module.exports = { connectRedis, getRedisClient };