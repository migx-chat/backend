const { getRedisClient } = require('../redis');

const ADMIN_KICK_COOLDOWN = 300; // 5 minutes (for kicked user)
const ADMIN_REJOIN_COOLDOWN = 180; // 3 minutes (admin cannot rejoin after kick)
const MAX_ADMIN_KICKS = 3;

async function adminKick(io, roomId, adminUsername, targetUsername, adminId, targetUserId) {
  const redis = getRedisClient();
  
  // Cooldown for kicked user in this room only
  const userCooldownKey = `cooldown:adminKick:${targetUsername}:${roomId}`;
  await redis.set(userCooldownKey, '1', { EX: ADMIN_KICK_COOLDOWN });

  // Track USER's total admin kicks across ALL rooms
  const userKickCountKey = `user:admin:kick:count:${targetUserId}`;
  const userKickCount = await redis.incr(userKickCountKey);

  // Check if user should be globally banned (exceeded max kicks)
  let userGlobalBanned = false;
  if (userKickCount >= MAX_ADMIN_KICKS) {
    const userGlobalBanKey = `ban:global:${targetUsername}`;
    await redis.set(userGlobalBanKey, 'true');
    userGlobalBanned = true;
  }

  // Track admin kick count for statistic purposes (not for ban)
  const adminKickCountKey = `admin:kick:count:${adminId}`;
  const adminKickCount = await redis.incr(adminKickCountKey);

  // Set admin rejoin cooldown (3 minutes)
  const adminCooldownKey = `admin:rejoin:cooldown:${adminId}:${roomId}`;
  await redis.set(adminCooldownKey, '1', { EX: ADMIN_REJOIN_COOLDOWN });

  return { 
    success: true, 
    userKickCount,
    userGlobalBanned,
    adminKickCount,
    adminUsername,
    targetUsername,
    adminCooldownSet: true
  };
}

async function isGloballyBanned(username) {
  const redis = getRedisClient();
  const globalBanKey = `ban:global:${username}`;
  const banned = await redis.get(globalBanKey);
  return banned === 'true';
}

async function isAdminGloballyBanned(adminId) {
  const redis = getRedisClient();
  const adminGlobalBanKey = `admin:global:banned:${adminId}`;
  const banned = await redis.get(adminGlobalBanKey);
  return banned === 'true';
}

async function clearUserKickCount(userId) {
  const redis = getRedisClient();
  const userKickCountKey = `user:admin:kick:count:${userId}`;
  
  await redis.del(userKickCountKey);
  
  return { success: true };
}

async function getUserKickCount(userId) {
  const redis = getRedisClient();
  const userKickCountKey = `user:admin:kick:count:${userId}`;
  const count = await redis.get(userKickCountKey);
  return parseInt(count) || 0;
}

async function getAdminKickCount(adminId) {
  const redis = getRedisClient();
  const adminKickCountKey = `admin:kick:count:${adminId}`;
  const count = await redis.get(adminKickCountKey);
  return parseInt(count) || 0;
}

module.exports = {
  adminKick,
  isGloballyBanned,
  isAdminGloballyBanned,
  clearUserKickCount,
  getUserKickCount,
  getAdminKickCount,
  ADMIN_KICK_COOLDOWN,
  ADMIN_REJOIN_COOLDOWN,
  MAX_ADMIN_KICKS
};
