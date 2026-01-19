const { getRedisClient } = require('../redis');
const { addCredits } = require('./creditService');

const CLAIM_CODE_KEY = (code) => `claim:code:${code}`;
const CLAIM_COOLDOWN_KEY = (userId) => `claim:cooldown:${userId}`;
const COOLDOWN_DURATION = 30 * 60;

const checkClaimCooldown = async (userId) => {
  try {
    const redis = getRedisClient();
    const cooldownKey = CLAIM_COOLDOWN_KEY(userId);
    const lastClaim = await redis.get(cooldownKey);
    
    if (lastClaim) {
      const ttl = await redis.ttl(cooldownKey);
      if (ttl > 0) {
        const remainingMinutes = Math.ceil(ttl / 60);
        return {
          allowed: false,
          remainingMinutes,
          remainingSeconds: ttl
        };
      }
    }
    
    return { allowed: true };
  } catch (error) {
    console.error('Error checking claim cooldown:', error);
    return { allowed: true };
  }
};

const setClaimCooldown = async (userId) => {
  try {
    const redis = getRedisClient();
    const cooldownKey = CLAIM_COOLDOWN_KEY(userId);
    await redis.set(cooldownKey, Date.now().toString());
    await redis.expire(cooldownKey, COOLDOWN_DURATION);
    return true;
  } catch (error) {
    console.error('Error setting claim cooldown:', error);
    return false;
  }
};

const validateClaimCode = async (code) => {
  try {
    const redis = getRedisClient();
    const codeKey = CLAIM_CODE_KEY(code);
    const codeData = await redis.hGetAll(codeKey);
    
    if (!codeData || Object.keys(codeData).length === 0) {
      return { valid: false, error: 'Code not found' };
    }
    
    const { min, max, expiredAt, maxUse, used } = codeData;
    
    const now = Date.now();
    if (parseInt(expiredAt) < now) {
      return { valid: false, error: 'Code expired' };
    }
    
    if (parseInt(used) >= parseInt(maxUse)) {
      return { valid: false, error: 'Code usage limit reached' };
    }
    
    return {
      valid: true,
      min: parseInt(min),
      max: parseInt(max),
      used: parseInt(used),
      maxUse: parseInt(maxUse)
    };
  } catch (error) {
    console.error('Error validating claim code:', error);
    return { valid: false, error: 'Validation failed' };
  }
};

const incrementCodeUsage = async (code) => {
  try {
    const redis = getRedisClient();
    const codeKey = CLAIM_CODE_KEY(code);
    await redis.hIncrBy(codeKey, 'used', 1);
    return true;
  } catch (error) {
    console.error('Error incrementing code usage:', error);
    return false;
  }
};

const processClaim = async (userId, code) => {
  const cooldownCheck = await checkClaimCooldown(userId);
  if (!cooldownCheck.allowed) {
    return {
      success: false,
      type: 'cooldown',
      remainingMinutes: cooldownCheck.remainingMinutes
    };
  }
  
  const codeValidation = await validateClaimCode(code);
  if (!codeValidation.valid) {
    return {
      success: false,
      type: 'invalid',
      error: codeValidation.error
    };
  }
  
  const { min, max } = codeValidation;
  const amount = Math.floor(Math.random() * (max - min + 1)) + min;
  
  const creditResult = await addCredits(userId, amount, 'claim', `Claimed from code: ${code}`);
  if (!creditResult.success) {
    return {
      success: false,
      type: 'error',
      error: 'Failed to add credits'
    };
  }
  
  await incrementCodeUsage(code);
  await setClaimCooldown(userId);
  
  return {
    success: true,
    amount,
    newBalance: creditResult.newBalance
  };
};

const createClaimCode = async (code, min, max, expiredAt, maxUse) => {
  try {
    const redis = getRedisClient();
    const codeKey = CLAIM_CODE_KEY(code);
    
    await redis.hSet(codeKey, {
      min: min.toString(),
      max: max.toString(),
      expiredAt: expiredAt.toString(),
      maxUse: maxUse.toString(),
      used: '0'
    });
    
    const ttl = Math.ceil((expiredAt - Date.now()) / 1000);
    if (ttl > 0) {
      await redis.expire(codeKey, ttl);
    }
    
    return { success: true, code };
  } catch (error) {
    console.error('Error creating claim code:', error);
    return { success: false, error: 'Failed to create code' };
  }
};

const deleteClaimCode = async (code) => {
  try {
    const redis = getRedisClient();
    const codeKey = CLAIM_CODE_KEY(code);
    await redis.del(codeKey);
    return { success: true };
  } catch (error) {
    console.error('Error deleting claim code:', error);
    return { success: false, error: 'Failed to delete code' };
  }
};

const getClaimCodeInfo = async (code) => {
  try {
    const redis = getRedisClient();
    const codeKey = CLAIM_CODE_KEY(code);
    const codeData = await redis.hGetAll(codeKey);
    
    if (!codeData || Object.keys(codeData).length === 0) {
      return null;
    }
    
    return {
      code,
      min: parseInt(codeData.min),
      max: parseInt(codeData.max),
      expiredAt: parseInt(codeData.expiredAt),
      maxUse: parseInt(codeData.maxUse),
      used: parseInt(codeData.used)
    };
  } catch (error) {
    console.error('Error getting claim code info:', error);
    return null;
  }
};

module.exports = {
  checkClaimCooldown,
  setClaimCooldown,
  validateClaimCode,
  incrementCodeUsage,
  processClaim,
  createClaimCode,
  deleteClaimCode,
  getClaimCodeInfo,
  COOLDOWN_DURATION
};
