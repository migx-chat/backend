const { getRedisClient } = require('../redis');
const crypto = require('crypto');

const SESSION_TTL = 86400; // 24 hours in seconds
const REFRESH_SESSION_TTL = 604800; // 7 days in seconds

function generateSid() {
  return crypto.randomUUID();
}

async function createSession(userId, userData, type = 'access') {
  const redis = getRedisClient();
  const sid = generateSid();
  
  const sessionData = {
    sid,
    userId,
    username: userData.username,
    role: userData.role,
    email: userData.email,
    deviceId: userData.deviceId || null,
    ip: userData.ip || null,
    type: type, // 'access' or 'refresh'
    createdAt: new Date().toISOString(),
    lastActive: new Date().toISOString()
  };

  const ttl = type === 'refresh' ? REFRESH_SESSION_TTL : SESSION_TTL;
  
  await redis.set(
    `session:${sid}`,
    JSON.stringify(sessionData),
    { EX: ttl }
  );

  // Track user's active sessions
  await redis.sAdd(`user:sessions:${userId}`, sid);
  await redis.expire(`user:sessions:${userId}`, REFRESH_SESSION_TTL);

  return { sid, sessionData };
}

async function getSession(sid) {
  const redis = getRedisClient();
  const data = await redis.get(`session:${sid}`);
  
  if (!data) {
    return null;
  }

  const session = JSON.parse(data);
  
  // Update last active timestamp
  session.lastActive = new Date().toISOString();
  await redis.set(`session:${sid}`, JSON.stringify(session), { KEEPTTL: true });
  
  return session;
}

async function deleteSession(sid) {
  const redis = getRedisClient();
  const session = await getSession(sid);
  
  if (session) {
    await redis.sRem(`user:sessions:${session.userId}`, sid);
    await redis.del(`session:${sid}`);
    return true;
  }
  
  return false;
}

async function deleteAllUserSessions(userId) {
  const redis = getRedisClient();
  const sids = await redis.sMembers(`user:sessions:${userId}`);
  
  for (const sid of sids) {
    await redis.del(`session:${sid}`);
  }
  
  await redis.del(`user:sessions:${userId}`);
  return sids.length;
}

async function getUserActiveSessions(userId) {
  const redis = getRedisClient();
  const sids = await redis.sMembers(`user:sessions:${userId}`);
  
  const sessions = [];
  for (const sid of sids) {
    const session = await getSession(sid);
    if (session) {
      sessions.push({
        sid,
        deviceId: session.deviceId,
        ip: session.ip,
        createdAt: session.createdAt,
        lastActive: session.lastActive
      });
    }
  }
  
  return sessions;
}

async function refreshSessionTTL(sid, type = 'access') {
  const redis = getRedisClient();
  const ttl = type === 'refresh' ? REFRESH_SESSION_TTL : SESSION_TTL;
  await redis.expire(`session:${sid}`, ttl);
}

module.exports = {
  generateSid,
  createSession,
  getSession,
  deleteSession,
  deleteAllUserSessions,
  getUserActiveSessions,
  refreshSessionTTL,
  SESSION_TTL,
  REFRESH_SESSION_TTL
};
