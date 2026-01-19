
const { getRedisClient } = require('../redis');

const ROOM_USERS_KEY = (roomId) => `room:users:${roomId}`;
const ROOM_SYSTEM_KEY = (roomId) => `room:system:${roomId}`;

const MAX_SYSTEM_MESSAGES = 50;

const { getRoomUsersFromTTL, getActiveUserCountInRoom } = require('./roomPresenceTTL');

/**
 * Add user to room presence - legacy support (kept for compatibility)
 */
const addUserToRoom = async (roomId, username) => {
  try {
    const client = getRedisClient();
    await client.sAdd(ROOM_USERS_KEY(roomId), username);
    return true;
  } catch (error) {
    console.error('Error adding user to room:', error);
    return false;
  }
};

/**
 * Remove user from room presence - legacy support (kept for compatibility)
 */
const removeUserFromRoom = async (roomId, username) => {
  try {
    const client = getRedisClient();
    await client.sRem(ROOM_USERS_KEY(roomId), username);
    return true;
  } catch (error) {
    console.error('Error removing user from room:', error);
    return false;
  }
};

/**
 * Get all users in room FROM REDIS SET (SINGLE SOURCE OF TRUTH)
 * Returns array of usernames (already stored as usernames in the Set)
 */
const getRoomUsers = async (roomId) => {
  try {
    const { getRedisClient } = require('../redis');
    const redis = getRedisClient();
    // Participants are stored as usernames directly in the Set
    const usernames = await redis.sMembers(`room:${roomId}:participants`);
    return usernames || [];
  } catch (error) {
    console.error('Error getting room users:', error);
    return [];
  }
};

/**
 * Get room user count FROM REDIS SET (SINGLE SOURCE OF TRUTH)
 */
const getRoomUserCount = async (roomId) => {
  try {
    const { getRedisClient } = require('../redis');
    const redis = getRedisClient();
    const key = `room:${roomId}:participants`;
    
    // Count members in the Set
    const count = await redis.sCard(key);
    return count || 0;
  } catch (error) {
    console.error('Error getting room user count:', error);
    return 0;
  }
};

/**
 * Check if user is in room
 */
const isUserInRoom = async (roomId, username) => {
  try {
    const client = getRedisClient();
    const isMember = await client.sIsMember(ROOM_USERS_KEY(roomId), username);
    return isMember;
  } catch (error) {
    console.error('Error checking user in room:', error);
    return false;
  }
};

/**
 * Add system message to room history (optional)
 */
const addSystemMessage = async (roomId, message) => {
  try {
    const client = getRedisClient();
    const messageData = JSON.stringify({
      message,
      timestamp: new Date().toISOString(),
      type: 'system'
    });
    await client.lPush(ROOM_SYSTEM_KEY(roomId), messageData);
    await client.lTrim(ROOM_SYSTEM_KEY(roomId), 0, MAX_SYSTEM_MESSAGES - 1);
    return true;
  } catch (error) {
    console.error('Error adding system message:', error);
    return false;
  }
};

/**
 * Get system messages for room
 */
const getSystemMessages = async (roomId, limit = 50) => {
  try {
    const client = getRedisClient();
    const messages = await client.lRange(ROOM_SYSTEM_KEY(roomId), 0, limit - 1);
    return messages.map(msg => JSON.parse(msg));
  } catch (error) {
    console.error('Error getting system messages:', error);
    return [];
  }
};

/**
 * Clear all users from room (cleanup)
 */
const clearRoomUsers = async (roomId) => {
  try {
    const client = getRedisClient();
    await client.del(ROOM_USERS_KEY(roomId));
    return true;
  } catch (error) {
    console.error('Error clearing room users:', error);
    return false;
  }
};

module.exports = {
  addUserToRoom,
  removeUserFromRoom,
  getRoomUsers,
  getRoomUserCount,
  isUserInRoom,
  addSystemMessage,
  getSystemMessages,
  clearRoomUsers
};
