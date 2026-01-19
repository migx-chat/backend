const { getRedisClient } = require('../redis');

const FLOOD_TTL = 3;
const FLOOD_LIMIT = 4;

const FLOOD_KEY = (username) => `flood:${username}`;
const GLOBAL_RATE_KEY = (userId) => `rate:global:${userId}`;
const GLOBAL_RATE_LIMIT = 30;
const GLOBAL_RATE_WINDOW = 60;

const checkFlood = async (username) => {
  try {
    const client = getRedisClient();
    if (!client) {
      return { allowed: true };
    }
    const key = FLOOD_KEY(username);
    const count = await client.incr(key);
    
    if (count === 1) {
      await client.expire(key, FLOOD_TTL);
    }
    
    if (count > FLOOD_LIMIT) {
      return { allowed: false, message: 'Slow down! Wait a moment before sending another message.' };
    }
    
    return { allowed: true };
  } catch (error) {
    console.error('Error checking flood:', error);
    return { allowed: true };
  }
};

const checkGlobalRateLimit = async (userId) => {
  try {
    const client = getRedisClient();
    if (!client) {
      return { allowed: true };
    }
    const key = GLOBAL_RATE_KEY(userId);
    const count = await client.incr(key);

    if (count === 1) {
      await client.expire(key, GLOBAL_RATE_WINDOW);
    }

    if (count > GLOBAL_RATE_LIMIT) {
      const ttl = await client.ttl(key);
      return { 
        allowed: false, 
        message: `Rate limit exceeded. Try again in ${ttl} seconds.`,
        retryAfter: ttl
      };
    }

    return { allowed: true, remaining: GLOBAL_RATE_LIMIT - count };
  } catch (error) {
    console.error('Error checking global rate limit:', error);
    return { allowed: true };
  }
};

const resetFlood = async (username) => {
  try {
    const client = getRedisClient();
    if (!client) {
      return true;
    }
    const key = FLOOD_KEY(username);
    await client.del(key);
    return true;
  } catch (error) {
    console.error('Error resetting flood:', error);
    return false;
  }
};

const checkTransferLimit = async (userId) => {
  try {
    const client = getRedisClient();
    if (!client) {
      return { allowed: true };
    }
    const key = `transfer:limit:${userId}`;
    const count = await client.incr(key);

    if (count === 1) {
      await client.expire(key, 60);
    }

    if (count > 5) {
      return { allowed: false, message: 'Transfer limit reached. Maximum 5 transfers per minute.' };
    }

    return { allowed: true, remaining: 5 - count };
  } catch (error) {
    console.error('Error checking transfer limit:', error);
    return { allowed: true };
  }
};

const checkGameLimit = async (userId) => {
  try {
    const client = getRedisClient();
    if (!client) {
      return { allowed: true };
    }
    const key = `game:limit:${userId}`;
    const count = await client.incr(key);

    if (count === 1) {
      await client.expire(key, 10);
    }

    if (count > 3) {
      return { allowed: false, message: 'Please wait before playing again.' };
    }

    return { allowed: true };
  } catch (error) {
    console.error('Error checking game limit:', error);
    return { allowed: true };
  }
};

module.exports = {
  checkFlood,
  checkGlobalRateLimit,
  resetFlood,
  checkTransferLimit,
  checkGameLimit,
  FLOOD_TTL,
  FLOOD_LIMIT
};