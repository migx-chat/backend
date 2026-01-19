const logger = require('../utils/logger');
const { getRedisClient } = require('../redis');
const { addCredits } = require('./creditService');
const { generateMessageId } = require('../utils/idGenerator');

const ACTIVE_VOUCHER_KEY = 'voucher:active';
const VOUCHER_COOLDOWN_KEY = (userId) => `voucher:cooldown:${userId}`;
const VOUCHER_CLAIMED_KEY = (code) => `voucher:claimed:${code}`;
const VOUCHER_POOL_KEY = 'voucher:pool';

const VOUCHER_CONFIG = {
  intervalMinutes: 30,  // Voucher appears every 30 minutes
  expirySeconds: 60,
  totalPool: 100,
  maxClaimers: 40,
  minClaimAmount: 1,
  maxClaimAmount: 3,
  userCooldownMinutes: 30,
  targetRoomNames: ['Migx', 'Voucher', 'Cafe Gaul Indo']  // Only broadcast to these rooms
};

let voucherInterval = null;
let ioInstance = null;

const generateRandomCode = () => {
  return Math.floor(1000000 + Math.random() * 9000000).toString();
};

const createNewVoucher = async () => {
  try {
    const redis = getRedisClient();
    const code = generateRandomCode();
    const expiresAt = Date.now() + (VOUCHER_CONFIG.expirySeconds * 1000);
    
    const voucherData = {
      code,
      totalPool: VOUCHER_CONFIG.totalPool.toString(),
      remainingPool: VOUCHER_CONFIG.totalPool.toString(),
      claimCount: '0',
      maxClaimers: VOUCHER_CONFIG.maxClaimers.toString(),
      expiresAt: expiresAt.toString(),
      createdAt: Date.now().toString()
    };
    
    await redis.hSet(ACTIVE_VOUCHER_KEY, voucherData);
    await redis.expire(ACTIVE_VOUCHER_KEY, VOUCHER_CONFIG.expirySeconds + 10);
    
    return { code, totalPool: VOUCHER_CONFIG.totalPool, expiresAt };
  } catch (error) {
    console.error('Error creating voucher:', error);
    return null;
  }
};

const getActiveVoucher = async () => {
  try {
    const redis = getRedisClient();
    const data = await redis.hGetAll(ACTIVE_VOUCHER_KEY);
    
    if (!data || !data.code) return null;
    
    const expiresAt = parseInt(data.expiresAt);
    if (Date.now() > expiresAt) {
      await redis.del(ACTIVE_VOUCHER_KEY);
      return null;
    }
    
    const remainingPool = parseInt(data.remainingPool) || 0;
    const claimCount = parseInt(data.claimCount) || 0;
    const maxClaimers = parseInt(data.maxClaimers) || VOUCHER_CONFIG.maxClaimers;
    
    if (remainingPool <= 0 || claimCount >= maxClaimers) {
      return null;
    }
    
    return {
      code: data.code,
      totalPool: parseInt(data.totalPool),
      remainingPool,
      claimCount,
      maxClaimers,
      expiresAt,
      remainingSeconds: Math.ceil((expiresAt - Date.now()) / 1000)
    };
  } catch (error) {
    console.error('Error getting active voucher:', error);
    return null;
  }
};

const checkUserCooldown = async (userId) => {
  try {
    const redis = getRedisClient();
    const cooldownKey = VOUCHER_COOLDOWN_KEY(userId);
    const ttl = await redis.ttl(cooldownKey);
    
    if (ttl > 0) {
      return {
        allowed: false,
        remainingMinutes: Math.ceil(ttl / 60),
        remainingSeconds: ttl
      };
    }
    
    return { allowed: true };
  } catch (error) {
    console.error('Error checking user cooldown:', error);
    return { allowed: true };
  }
};

const setUserCooldown = async (userId) => {
  try {
    const redis = getRedisClient();
    const cooldownKey = VOUCHER_COOLDOWN_KEY(userId);
    await redis.set(cooldownKey, Date.now().toString());
    await redis.expire(cooldownKey, VOUCHER_CONFIG.userCooldownMinutes * 60);
    return true;
  } catch (error) {
    console.error('Error setting user cooldown:', error);
    return false;
  }
};

const hasUserClaimed = async (code, userId) => {
  try {
    const redis = getRedisClient();
    const claimedKey = VOUCHER_CLAIMED_KEY(code);
    const isClaimed = await redis.sIsMember(claimedKey, userId.toString());
    return isClaimed;
  } catch (error) {
    console.error('Error checking if user claimed:', error);
    return false;
  }
};

const markUserClaimed = async (code, userId) => {
  try {
    const redis = getRedisClient();
    const claimedKey = VOUCHER_CLAIMED_KEY(code);
    await redis.sAdd(claimedKey, userId.toString());
    await redis.expire(claimedKey, VOUCHER_CONFIG.expirySeconds + 60);
    return true;
  } catch (error) {
    console.error('Error marking user claimed:', error);
    return false;
  }
};

const claimVoucher = async (userId, inputCode) => {
  const cooldownCheck = await checkUserCooldown(userId);
  if (!cooldownCheck.allowed) {
    return {
      success: false,
      type: 'cooldown',
      remainingMinutes: cooldownCheck.remainingMinutes
    };
  }
  
  const redis = getRedisClient();
  const lockKey = `voucher:lock:${inputCode}`;
  
  try {
    const locked = await redis.set(lockKey, '1', { NX: true, EX: 5 });
    if (!locked) {
      return {
        success: false,
        type: 'busy',
        error: 'Please try again'
      };
    }
    
    const activeVoucher = await getActiveVoucher();
    if (!activeVoucher) {
      await redis.del(lockKey);
      return {
        success: false,
        type: 'expired',
        error: 'No active voucher or pool is empty'
      };
    }
    
    if (activeVoucher.code !== inputCode) {
      await redis.del(lockKey);
      return {
        success: false,
        type: 'invalid',
        error: 'Invalid code'
      };
    }
    
    const alreadyClaimed = await hasUserClaimed(inputCode, userId);
    if (alreadyClaimed) {
      await redis.del(lockKey);
      return {
        success: false,
        type: 'already_claimed',
        error: 'You already claimed this voucher'
      };
    }
    
    const remainingPool = activeVoucher.remainingPool;
    const remainingClaimers = activeVoucher.maxClaimers - activeVoucher.claimCount;
    
    let claimAmount;
    if (remainingClaimers <= 1) {
      claimAmount = remainingPool;
    } else {
      const maxPossible = Math.min(
        VOUCHER_CONFIG.maxClaimAmount,
        Math.floor(remainingPool * 0.5)
      );
      const minPossible = Math.max(VOUCHER_CONFIG.minClaimAmount, 1);
      
      if (maxPossible <= minPossible) {
        claimAmount = Math.min(remainingPool, minPossible);
      } else {
        claimAmount = Math.floor(Math.random() * (maxPossible - minPossible + 1)) + minPossible;
      }
    }
    
    claimAmount = Math.min(claimAmount, remainingPool);
    
    if (claimAmount <= 0) {
      await redis.del(lockKey);
      return {
        success: false,
        type: 'pool_empty',
        error: 'Voucher pool is empty'
      };
    }
    
    const creditResult = await addCredits(userId, claimAmount, 'voucher_claim', `Claimed voucher: ${inputCode}`);
    if (!creditResult.success) {
      await redis.del(lockKey);
      return {
        success: false,
        type: 'error',
        error: 'Failed to add credits'
      };
    }
    
    const newRemainingPool = remainingPool - claimAmount;
    const newClaimCount = activeVoucher.claimCount + 1;
    
    await redis.hSet(ACTIVE_VOUCHER_KEY, {
      remainingPool: newRemainingPool.toString(),
      claimCount: newClaimCount.toString()
    });
    
    await markUserClaimed(inputCode, userId);
    await setUserCooldown(userId);
    await redis.del(lockKey);
    
    return {
      success: true,
      amount: claimAmount,
      newBalance: creditResult.newBalance,
      poolRemaining: newRemainingPool,
      claimersRemaining: activeVoucher.maxClaimers - newClaimCount
    };
  } catch (error) {
    console.error('Error claiming voucher:', error);
    await redis.del(lockKey);
    return {
      success: false,
      type: 'error',
      error: 'Failed to claim voucher'
    };
  }
};

const broadcastVoucherAnnouncement = async (voucher) => {
  if (!ioInstance) {
    logger.info('No IO instance for voucher broadcast');
    return;
  }
  
  const message = `🎁 FREE CREDIT!! Total ${voucher.totalPool.toLocaleString()} COINS! CMD type /c ${voucher.code} to claim`;
  
  const announcement = {
    id: generateMessageId(),
    message,
    messageType: 'voucher',
    type: 'voucher',
    timestamp: new Date().toISOString(),
    voucherCode: voucher.code,
    voucherCodeColor: '#FF0000',
    voucherPool: voucher.totalPool,
    maxClaimers: VOUCHER_CONFIG.maxClaimers,
    expiresIn: VOUCHER_CONFIG.expirySeconds
  };
  
  ioInstance.emit('system:voucher', announcement);
  
  const pool = require('../db/db');
  try {
    // Get target room IDs by name
    const targetRooms = VOUCHER_CONFIG.targetRoomNames;
    const placeholders = targetRooms.map((_, i) => `$${i + 1}`).join(', ');
    const result = await pool.query(
      `SELECT id, name FROM rooms WHERE name IN (${placeholders})`,
      targetRooms
    );
    
    logger.info(`📢 Broadcasting voucher to ${result.rows.length} target rooms: ${result.rows.map(r => `${r.name}(${r.id})`).join(', ')}`);
    
    for (const room of result.rows) {
      const roomSocketId = `room:${room.id}`;
      logger.info(`📢 Emitting voucher to socket room: ${roomSocketId}`);
      
      ioInstance.to(roomSocketId).emit('chat:message', {
        id: generateMessageId(),
        roomId: room.id.toString(),
        message,
        messageType: 'voucher',
        type: 'voucher',
        voucherCode: voucher.code,
        voucherCodeColor: '#FF0000',
        timestamp: new Date().toISOString(),
        expiresIn: VOUCHER_CONFIG.expirySeconds
      });
    }
  } catch (error) {
    console.error('Error broadcasting to rooms:', error);
  }
  
  logger.info(`📢 Voucher broadcast: ${voucher.code} - Pool ${voucher.totalPool} COINS for ${VOUCHER_CONFIG.maxClaimers} users`);
};

const startVoucherGenerator = (io) => {
  ioInstance = io;
  
  if (voucherInterval) {
    clearInterval(voucherInterval);
  }
  
  const generateAndBroadcast = async () => {
    const voucher = await createNewVoucher();
    if (voucher) {
      await broadcastVoucherAnnouncement(voucher);
    }
  };
  
  generateAndBroadcast();
  
  voucherInterval = setInterval(generateAndBroadcast, VOUCHER_CONFIG.intervalMinutes * 60 * 1000);
  
  logger.info(`🎫 Voucher generator started - every ${VOUCHER_CONFIG.intervalMinutes} minutes, pool: ${VOUCHER_CONFIG.totalPool} COINS`);
};

const stopVoucherGenerator = () => {
  if (voucherInterval) {
    clearInterval(voucherInterval);
    voucherInterval = null;
  }
  logger.info('🎫 Voucher generator stopped');
};

const updateVoucherConfig = (config) => {
  Object.assign(VOUCHER_CONFIG, config);
  logger.info('🎫 Voucher config updated:', VOUCHER_CONFIG);
};

module.exports = {
  createNewVoucher,
  getActiveVoucher,
  claimVoucher,
  startVoucherGenerator,
  stopVoucherGenerator,
  updateVoucherConfig,
  VOUCHER_CONFIG
};
