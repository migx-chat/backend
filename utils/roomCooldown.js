const { getRedisClient } = require('../redis');

async function checkJoinAllowed(username, roomId, userId) {
  const redis = getRedisClient();

  // Check if USER is globally banned
  const globalBanKey = `ban:global:${username}`;
  const isGlobalBanned = await redis.get(globalBanKey);
  if (isGlobalBanned === 'true') {
    return {
      allowed: false,
      reason: 'You are globally banned from all rooms.',
      type: 'globalBan'
    };
  }

  // Check if ADMIN is globally banned due to excessive kicking
  if (userId) {
    const adminGlobalBanKey = `admin:global:banned:${userId}`;
    const isAdminBanned = await redis.get(adminGlobalBanKey);
    if (isAdminBanned === 'true') {
      return {
        allowed: false,
        reason: 'You are banned from all rooms due to excessive kicking.',
        type: 'adminGlobalBan'
      };
    }
  }

  // Check if user was kicked (admin kick cooldown)
  const adminCooldownKey = `cooldown:adminKick:${username}:${roomId}`;
  const adminCooldown = await redis.ttl(adminCooldownKey);
  if (adminCooldown > 0) {
    const minutes = Math.ceil(adminCooldown / 60);
    return {
      allowed: false,
      reason: `You have been kicked by admin. Please wait ${minutes} minute(s) before rejoining.`,
      type: 'adminKick',
      remainingSeconds: adminCooldown
    };
  }

  // Check if user was vote-kicked
  const voteCooldownKey = `cooldown:voteKick:${username}:${roomId}`;
  const voteCooldown = await redis.ttl(voteCooldownKey);
  if (voteCooldown > 0) {
    const minutes = Math.ceil(voteCooldown / 60);
    return {
      allowed: false,
      reason: `You have been vote-kicked. Please wait ${minutes} minute(s) before rejoining.`,
      type: 'voteKick',
      remainingSeconds: voteCooldown
    };
  }

  // Check if admin is under cooldown from kicking (cannot rejoin for 3 minutes)
  if (userId) {
    const adminRejoinCooldownKey = `admin:rejoin:cooldown:${userId}:${roomId}`;
    const adminRejoinCooldown = await redis.ttl(adminRejoinCooldownKey);
    if (adminRejoinCooldown > 0) {
      const minutes = Math.ceil(adminRejoinCooldown / 60);
      return {
        allowed: false,
        reason: `You must wait ${minutes} minute(s) before rejoining this room.`,
        type: 'adminRejoinCooldown',
        remainingSeconds: adminRejoinCooldown
      };
    }
  }

  return { allowed: true };
}

async function clearAdminCooldown(username, roomId) {
  const redis = getRedisClient();
  const cooldownKey = `cooldown:adminKick:${username}:${roomId}`;
  await redis.del(cooldownKey);
  return { success: true };
}

async function clearVoteCooldown(username, roomId) {
  const redis = getRedisClient();
  const cooldownKey = `cooldown:voteKick:${username}:${roomId}`;
  await redis.del(cooldownKey);
  return { success: true };
}

async function getCooldownStatus(username, roomId) {
  const redis = getRedisClient();
  
  const globalBanKey = `ban:global:${username}`;
  const isGlobalBanned = await redis.get(globalBanKey);
  
  const adminCooldownKey = `cooldown:adminKick:${username}:${roomId}`;
  const adminCooldown = await redis.ttl(adminCooldownKey);
  
  const voteCooldownKey = `cooldown:voteKick:${username}:${roomId}`;
  const voteCooldown = await redis.ttl(voteCooldownKey);
  
  return {
    isGlobalBanned: isGlobalBanned === 'true',
    adminKickCooldown: adminCooldown > 0 ? adminCooldown : 0,
    voteKickCooldown: voteCooldown > 0 ? voteCooldown : 0
  };
}

module.exports = {
  checkJoinAllowed,
  clearAdminCooldown,
  clearVoteCooldown,
  getCooldownStatus
};
