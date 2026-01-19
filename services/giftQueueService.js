const logger = require('../utils/logger');
const { getRedisClient } = require('../redis');
const { query } = require('../db/db');

const GIFT_QUEUE_KEY = 'queue:gifts';
const GIFT_RETRY_KEY = 'queue:gifts:retry';
const MAX_RETRIES = 3;

const queueGiftForPersistence = async (giftData) => {
  try {
    const redis = getRedisClient();
    const payload = JSON.stringify({
      ...giftData,
      queuedAt: Date.now(),
      retries: 0
    });
    
    await redis.rPush(GIFT_QUEUE_KEY, payload);
    logger.info(`🎁 Gift queued for persistence: ${giftData.giftName} from ${giftData.senderUsername} to ${giftData.receiverUsername}`);
    return true;
  } catch (error) {
    console.error('❌ Failed to queue gift:', error.message);
    return false;
  }
};

const persistGiftToDatabase = async (giftData) => {
  try {
    await query(
      `INSERT INTO user_gifts (sender_id, receiver_id, gift_name, gift_icon, gift_cost)
       VALUES ($1, $2, $3, $4, $5)`,
      [giftData.senderId, giftData.receiverId, giftData.giftName, giftData.giftIcon, giftData.giftCost]
    );
    
    await query(
      `INSERT INTO credit_logs (from_user_id, to_user_id, amount, transaction_type, description, from_username, to_username, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP)`,
      [giftData.senderId, giftData.receiverId, giftData.giftCost, 'gift', `Gift: ${giftData.giftName} to ${giftData.receiverUsername}`, giftData.senderUsername, giftData.receiverUsername]
    );
    
    logger.info(`✅ Gift persisted to DB: ${giftData.giftName}`);
    return true;
  } catch (error) {
    console.error('❌ Failed to persist gift to DB:', error.message);
    return false;
  }
};

const processGiftQueue = async () => {
  const redis = getRedisClient();
  
  try {
    const payload = await redis.lPop(GIFT_QUEUE_KEY);
    if (!payload) return;
    
    const giftData = JSON.parse(payload);
    const success = await persistGiftToDatabase(giftData);
    
    if (!success) {
      giftData.retries = (giftData.retries || 0) + 1;
      
      if (giftData.retries < MAX_RETRIES) {
        await redis.rPush(GIFT_RETRY_KEY, JSON.stringify(giftData));
        logger.info(`⏳ Gift queued for retry (${giftData.retries}/${MAX_RETRIES})`);
      } else {
        console.error(`❌ Gift failed after ${MAX_RETRIES} retries:`, giftData);
      }
    }
  } catch (error) {
    console.error('❌ Error processing gift queue:', error.message);
  }
};

const processRetryQueue = async () => {
  const redis = getRedisClient();
  
  try {
    const payload = await redis.lPop(GIFT_RETRY_KEY);
    if (!payload) return;
    
    const giftData = JSON.parse(payload);
    const success = await persistGiftToDatabase(giftData);
    
    if (!success && giftData.retries < MAX_RETRIES) {
      giftData.retries++;
      await redis.rPush(GIFT_RETRY_KEY, JSON.stringify(giftData));
    }
  } catch (error) {
    console.error('❌ Error processing retry queue:', error.message);
  }
};

const startGiftQueueProcessor = () => {
  setInterval(async () => {
    await processGiftQueue();
  }, 100);
  
  setInterval(async () => {
    await processRetryQueue();
  }, 5000);
  
  logger.info('🎁 Gift queue processor started');
};

const deductCreditsAtomic = async (userId, amount) => {
  const redis = getRedisClient();
  const balanceKey = `user:${userId}:credits`;
  
  // First ensure balance is synced from DB to Redis
  let currentBalance = await redis.get(balanceKey);
  if (currentBalance === null) {
    // Sync from database first
    const dbBalance = await syncBalanceFromDb(userId);
    if (dbBalance === null) {
      console.error('❌ Failed to sync balance from DB for user:', userId);
      return null;
    }
    currentBalance = dbBalance.toString();
  }
  
  const luaScript = `
    local current = tonumber(redis.call('GET', KEYS[1]) or '0')
    local amount = tonumber(ARGV[1])
    if current >= amount then
      redis.call('DECRBY', KEYS[1], amount)
      return current - amount
    else
      return -1
    end
  `;
  
  try {
    const result = await redis.eval(luaScript, {
      keys: [balanceKey],
      arguments: [amount.toString()]
    });
    
    return result >= 0 ? result : null;
  } catch (error) {
    console.error('❌ Redis atomic deduct failed:', error.message);
    return null;
  }
};

const syncBalanceFromDb = async (userId) => {
  try {
    const redis = getRedisClient();
    const result = await query('SELECT credits FROM users WHERE id = $1', [userId]);
    
    if (result.rows.length > 0) {
      const balanceKey = `user:${userId}:credits`;
      await redis.set(balanceKey, result.rows[0].credits.toString());
      return result.rows[0].credits;
    }
    return 0;
  } catch (error) {
    console.error('❌ Failed to sync balance from DB:', error.message);
    return null;
  }
};

const getBalanceFromRedis = async (userId) => {
  const redis = getRedisClient();
  const balanceKey = `user:${userId}:credits`;
  
  let balance = await redis.get(balanceKey);
  
  if (balance === null) {
    balance = await syncBalanceFromDb(userId);
  }
  
  return parseInt(balance) || 0;
};

const queueBalanceSyncToDb = async (userId, newBalance) => {
  try {
    await query('UPDATE users SET credits = $1 WHERE id = $2', [newBalance, userId]);
    return true;
  } catch (error) {
    console.error('❌ Failed to sync balance to DB:', error.message);
    return false;
  }
};

module.exports = {
  queueGiftForPersistence,
  startGiftQueueProcessor,
  deductCreditsAtomic,
  getBalanceFromRedis,
  syncBalanceFromDb,
  queueBalanceSyncToDb
};
