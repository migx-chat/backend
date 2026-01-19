/**
 * Room Presence with 6-hour TTL System
 * Using Redis with EXPIRE for ephemeral presence tracking
 * Key format: room:{roomId}:user:{userId}
 */

const { getRedisClient } = require('../redis');

const TTL_SECONDS = 1800; // 30 minutes (reduced from 6 hours to prevent stale presence issues)

/**
 * Store user presence in room with 30-minute TTL
 */
const storeUserPresence = async (roomId, userId, socketId, username) => {
  try {
    const client = getRedisClient();
    const key = `room:${roomId}:user:${userId}`;
    const presenceData = JSON.stringify({
      socketId,
      userId,
      roomId,
      username,
      lastActiveAt: Date.now(),
      joinedAt: Date.now()
    });
    
    await client.setEx(key, TTL_SECONDS, presenceData);
    console.log(`✅ Stored presence: ${key} (TTL: ${TTL_SECONDS}s)`);
    return true;
  } catch (error) {
    console.error('Error storing user presence:', error);
    return false;
  }
};

/**
 * Update last active time and refresh TTL
 */
const updatePresenceActivity = async (roomId, userId) => {
  try {
    const client = getRedisClient();
    const key = `room:${roomId}:user:${userId}`;
    
    // Get existing data
    const existingData = await client.get(key);
    if (!existingData) {
      console.log(`⚠️  No presence data found for ${key}`);
      return false;
    }
    
    const presenceData = JSON.parse(existingData);
    presenceData.lastActiveAt = Date.now();
    
    // Refresh TTL
    await client.setEx(key, TTL_SECONDS, JSON.stringify(presenceData));
    console.log(`♻️  Refreshed presence: ${key} (TTL reset to ${TTL_SECONDS}s)`);
    return true;
  } catch (error) {
    console.error('Error updating presence activity:', error);
    return false;
  }
};

/**
 * Remove user presence from room
 */
const removeUserPresence = async (roomId, userId) => {
  try {
    const client = getRedisClient();
    const key = `room:${roomId}:user:${userId}`;
    await client.del(key);
    console.log(`❌ Removed presence: ${key}`);
    return true;
  } catch (error) {
    console.error('Error removing user presence:', error);
    return false;
  }
};

/**
 * Remove user presence from ALL rooms (for logout/account switch)
 */
const removeAllUserPresence = async (userId) => {
  try {
    const client = getRedisClient();
    const pattern = `room:*:user:${userId}`;
    const keys = await client.keys(pattern);
    
    if (keys.length > 0) {
      await client.del(keys);
      console.log(`🧹 Cleared ${keys.length} presence keys for user ${userId}`);
    }
    return keys.length;
  } catch (error) {
    console.error('Error removing all user presence:', error);
    return 0;
  }
};

/**
 * Get user presence data
 */
const getUserPresence = async (roomId, userId) => {
  try {
    const client = getRedisClient();
    const key = `room:${roomId}:user:${userId}`;
    const data = await client.get(key);
    return data ? JSON.parse(data) : null;
  } catch (error) {
    console.error('Error getting user presence:', error);
    return null;
  }
};

/**
 * Get all users in room (from TTL-based keys)
 */
const getRoomUsersFromTTL = async (roomId) => {
  try {
    const client = getRedisClient();
    const pattern = `room:${roomId}:user:*`;
    const keys = await client.keys(pattern);
    
    const users = [];
    for (const key of keys) {
      const data = await client.get(key);
      if (data) {
        users.push(JSON.parse(data));
      }
    }
    return users;
  } catch (error) {
    console.error('Error getting room users from TTL:', error);
    return [];
  }
};

/**
 * Get count of active users in room
 */
const getActiveUserCountInRoom = async (roomId) => {
  try {
    const client = getRedisClient();
    const pattern = `room:${roomId}:user:*`;
    const keys = await client.keys(pattern);
    return keys.length;
  } catch (error) {
    console.error('Error getting active user count:', error);
    return 0;
  }
};

/**
 * Check if user presence exists
 */
const isUserPresenceActive = async (roomId, userId) => {
  try {
    const client = getRedisClient();
    const key = `room:${roomId}:user:${userId}`;
    const exists = await client.exists(key);
    return exists === 1;
  } catch (error) {
    console.error('Error checking user presence:', error);
    return false;
  }
};

/**
 * Cleanup expired presences - called by server cleanup job
 * Returns list of users that expired
 */
const cleanupExpiredPresences = async (roomId) => {
  try {
    const client = getRedisClient();
    const pattern = `room:${roomId}:user:*`;
    const keys = await client.keys(pattern);
    
    const expiredUsers = [];
    for (const key of keys) {
      const exists = await client.exists(key);
      if (exists === 0) {
        // Key expired naturally or was deleted
        const match = key.match(/room:(\d+):user:(\d+)/);
        if (match) {
          expiredUsers.push({
            roomId: parseInt(match[1]),
            userId: parseInt(match[2])
          });
        }
      }
    }
    
    return expiredUsers;
  } catch (error) {
    console.error('Error cleaning up expired presences:', error);
    return [];
  }
};

/**
 * Get all active rooms with users
 */
const getActiveRooms = async () => {
  try {
    const client = getRedisClient();
    const pattern = 'room:*:user:*';
    const keys = await client.keys(pattern);
    
    const roomsMap = new Map();
    for (const key of keys) {
      const match = key.match(/room:(\d+):user:/);
      if (match) {
        const roomId = match[1];
        if (!roomsMap.has(roomId)) {
          roomsMap.set(roomId, []);
        }
      }
    }
    
    return Array.from(roomsMap.keys());
  } catch (error) {
    console.error('Error getting active rooms:', error);
    return [];
  }
};

module.exports = {
  storeUserPresence,
  updatePresenceActivity,
  removeUserPresence,
  removeAllUserPresence,
  getUserPresence,
  getRoomUsersFromTTL,
  getActiveUserCountInRoom,
  isUserPresenceActive,
  cleanupExpiredPresences,
  getActiveRooms,
  TTL_SECONDS
};
