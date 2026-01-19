const { getRedisClient } = require('../redis');

const DEFAULT_TTL = 300;
const ONLINE_PRESENCE_TTL = 180;

const RECENT_ROOMS_KEY = (username) => `recent:${username}`;
const USER_ROOMS_KEY = (username) => `user:rooms:${username}`;
const ROOM_USERS_KEY = (roomId) => `room:users:${roomId}`;
const FAVORITE_ROOMS_KEY = (username) => `user:${username}:favorites`;
const HOT_ROOMS_KEY = 'hot:rooms';
const ROOM_ACTIVE_KEY = (roomId) => `room:active:${roomId}`;

const MAX_RECENT_ROOMS = 10;

const setPresence = async (username, status) => {
  try {
    const redis = getRedisClient();
    await redis.set(`presence:${username}`, status);
    if (status === 'online') {
      await redis.expire(`presence:${username}`, ONLINE_PRESENCE_TTL);
    } else if (status === 'away' || status === 'busy') {
      await redis.persist(`presence:${username}`);
    } else if (status === 'offline') {
      await redis.del(`presence:${username}`);
    }
    return true;
  } catch (error) {
    console.error('Error setting presence:', error);
    return false;
  }
};

const getPresence = async (username) => {
  try {
    const redis = getRedisClient();
    const status = await redis.get(`presence:${username}`);
    return status || 'offline';
  } catch (error) {
    console.error('Error getting presence:', error);
    return 'offline';
  }
};

const removePresence = async (username) => {
  try {
    const redis = getRedisClient();
    await redis.del(`presence:${username}`);
    return true;
  } catch (error) {
    console.error('Error removing presence:', error);
    return false;
  }
};

const refreshOnlinePresence = async (username) => {
  try {
    const redis = getRedisClient();
    const currentPresence = await getPresence(username);
    if (currentPresence === 'online') {
      await redis.expire(`presence:${username}`, ONLINE_PRESENCE_TTL);
    }
    return true;
  } catch (error) {
    console.error('Error refreshing online presence:', error);
    return false;
  }
};

const setSession = async (username, socketId) => {
  try {
    const redis = getRedisClient();
    await redis.set(`session:${username}`, socketId);
    await redis.expire(`session:${username}`, DEFAULT_TTL);
    return true;
  } catch (error) {
    console.error('Error setting session:', error);
    return false;
  }
};

const getSession = async (username) => {
  try {
    const redis = getRedisClient();
    const socketId = await redis.get(`session:${username}`);
    return socketId;
  } catch (error) {
    console.error('Error getting session:', error);
    return null;
  }
};

const removeSession = async (username) => {
  try {
    const redis = getRedisClient();
    await redis.del(`session:${username}`);
    return true;
  } catch (error) {
    console.error('Error removing session:', error);
    return false;
  }
};

const getRoomMembers = async (roomId) => {
  try {
    const redis = getRedisClient();
    const members = await redis.sMembers(`room:${roomId}:members`);
    return members || [];
  } catch (error) {
    console.error('Error getting room members:', error);
    return [];
  }
};

const addRoomMember = async (roomId, username) => {
  try {
    const redis = getRedisClient();
    await redis.sAdd(`room:${roomId}:members`, username);
    await redis.expire(`room:${roomId}:members`, DEFAULT_TTL);
    return true;
  } catch (error) {
    console.error('Error adding room member:', error);
    return false;
  }
};

const removeRoomMember = async (roomId, username) => {
  try {
    const redis = getRedisClient();
    await redis.sRem(`room:${roomId}:members`, username);
    return true;
  } catch (error) {
    console.error('Error removing room member:', error);
    return false;
  }
};

const setRoomUsers = async (roomId, users) => {
  try {
    const redis = getRedisClient();
    const key = `room:users:${roomId}`;
    await redis.del(key);
    if (users.length > 0) {
      const userData = users.map(u => JSON.stringify(u));
      await redis.sAdd(key, ...userData);
      await redis.expire(key, DEFAULT_TTL);
    }
    return true;
  } catch (error) {
    console.error('Error setting room users:', error);
    return false;
  }
};

const getRoomUsers = async (roomId) => {
  try {
    const redis = getRedisClient();
    const members = await redis.sMembers(`room:users:${roomId}`);
    return members.map(m => {
      try {
        return JSON.parse(m);
      } catch {
        return { username: m };
      }
    });
  } catch (error) {
    console.error('Error getting room users:', error);
    return [];
  }
};

const addUserToRoom = async (roomId, username) => {
  try {
    const redis = getRedisClient();
    const key = `room:users:${roomId}`;
    await redis.sAdd(key, username);
    await redis.expire(key, DEFAULT_TTL);
    return true;
  } catch (error) {
    console.error('Error adding user to room:', error);
    return false;
  }
};

const removeUserFromRoom = async (roomId, username) => {
  try {
    const redis = getRedisClient();
    const key = `room:users:${roomId}`;
    await redis.sRem(key, username);
    return true;
  } catch (error) {
    console.error('Error removing user from room:', error);
    return false;
  }
};

const getRoomUsersList = async (roomId) => {
  try {
    const redis = getRedisClient();
    const key = `room:users:${roomId}`;
    return await redis.sMembers(key);
  } catch (error) {
    console.error('Error getting room users list:', error);
    return [];
  }
};

const getRoomParticipants = async (roomId) => {
  try {
    const redis = getRedisClient();
    const key = `room:${roomId}:participants`;
    
    // Get all participants from SET (single source of truth)
    const participants = await redis.sMembers(key);
    return participants || [];
  } catch (error) {
    console.error('Error getting room participants:', error);
    return [];
  }
};

const addRoomParticipant = async (roomId, username) => {
  try {
    const redis = getRedisClient();
    const key = `room:${roomId}:participants`;
    
    // Add username to SET
    await redis.sAdd(key, username);
    return true;
  } catch (error) {
    console.error('Error adding room participant:', error);
    return false;
  }
};

const removeRoomParticipant = async (roomId, username) => {
  try {
    const redis = getRedisClient();
    const key = `room:${roomId}:participants`;
    
    // Remove username from SET
    await redis.sRem(key, username);
    return true;
  } catch (error) {
    console.error('Error removing room participant:', error);
    return false;
  }
};

const getRoomParticipantsWithNames = async (roomId, excludeUsername = null) => {
  try {
    const redis = getRedisClient();
    const key = `room:${roomId}:participants`;
    
    // Get all participants from SET
    let participants = await redis.sMembers(key);
    
    if (!participants || participants.length === 0) {
      return [];
    }
    
    // Filter out excluded user if provided
    if (excludeUsername) {
      participants = participants.filter(u => u !== excludeUsername);
    }
    
    return participants;
  } catch (error) {
    console.error('Error getting room participants with names:', error);
    return [];
  }
};

const clearRoomParticipants = async (roomId) => {
  try {
    const redis = getRedisClient();
    await redis.del(`room:${roomId}:participants`);
    return true;
  } catch (error) {
    console.error('Error clearing room participants:', error);
    return false;
  }
};

const getUserCurrentRoom = async (username) => {
  try {
    const redis = getRedisClient();
    const roomId = await redis.get(`room:userRoom:${username}`);
    return roomId;
  } catch (error) {
    console.error('Error getting user current room:', error);
    return null;
  }
};

const getRoomParticipantCount = async (roomId) => {
  try {
    const redis = getRedisClient();
    const count = await redis.sCard(`room:participants:${roomId}`);
    return count || 0;
  } catch (error) {
    console.error('Error getting room participant count:', error);
    return 0;
  }
};

const setUserRoom = async (username, roomId) => {
  try {
    const redis = getRedisClient();
    await redis.set(`user:room:${username}`, roomId.toString());
    await redis.expire(`user:room:${username}`, DEFAULT_TTL);
    return true;
  } catch (error) {
    console.error('Error setting user room:', error);
    return false;
  }
};

const getUserRoom = async (username) => {
  try {
    const redis = getRedisClient();
    return await redis.get(`user:room:${username}`);
  } catch (error) {
    console.error('Error getting user room:', error);
    return null;
  }
};

const setFlood = async (username, ttlSeconds = DEFAULT_TTL) => {
  try {
    const redis = getRedisClient();
    await redis.set(`flood:${username}`, '1');
    await redis.expire(`flood:${username}`, ttlSeconds);
    return true;
  } catch (error) {
    console.error('Error setting flood:', error);
    return false;
  }
};

const checkFlood = async (username) => {
  try {
    const redis = getRedisClient();
    const exists = await redis.exists(`flood:${username}`);
    return exists === 1;
  } catch (error) {
    console.error('Error checking flood:', error);
    return false;
  }
};

const clearFlood = async (username) => {
  try {
    const redis = getRedisClient();
    await redis.del(`flood:${username}`);
    return true;
  } catch (error) {
    console.error('Error clearing flood:', error);
    return false;
  }
};

const setTempKick = async (username, roomId) => {
  try {
    const redis = getRedisClient();
    await redis.set(`room:kick:${username}`, roomId.toString());
    await redis.expire(`room:kick:${username}`, DEFAULT_TTL);
    return true;
  } catch (error) {
    console.error('Error setting temp kick:', error);
    return false;
  }
};

const isTempKicked = async (username) => {
  try {
    const redis = getRedisClient();
    const roomId = await redis.get(`room:kick:${username}`);
    return roomId ? { kicked: true, roomId } : { kicked: false };
  } catch (error) {
    console.error('Error checking temp kick:', error);
    return { kicked: false };
  }
};

const clearTempKick = async (username) => {
  try {
    const redis = getRedisClient();
    await redis.del(`room:kick:${username}`);
    return true;
  } catch (error) {
    console.error('Error clearing temp kick:', error);
    return false;
  }
};

const addUserRoom = async (username, roomId, roomName) => {
  try {
    const redis = getRedisClient();
    const roomData = JSON.stringify({ id: roomId.toString(), roomId: roomId.toString(), name: roomName, roomName, joinedAt: Date.now() });
    await redis.sAdd(`user:rooms:${username}`, roomData);
    return true;
  } catch (error) {
    console.error('Error adding user room:', error);
    return false;
  }
};

const removeUserRoom = async (username, roomId) => {
  try {
    const redis = getRedisClient();
    const userRoomsKey = `user:rooms:${username}`;
    const rooms = await redis.sMembers(userRoomsKey);
    const roomIdStr = roomId.toString();

    for (const room of rooms) {
      try {
        // Try to parse as JSON
        const roomData = JSON.parse(room);
        if (roomData.id === roomIdStr || roomData.roomId === roomIdStr) {
          await redis.sRem(userRoomsKey, room);
        }
      } catch (e) {
        // If not JSON, it's a bare ID string - check and remove
        if (room === roomIdStr) {
          await redis.sRem(userRoomsKey, room);
        }
      }
    }

    return true;
  } catch (error) {
    console.error('Error removing user room:', error);
    return false;
  }
};

const getUserRooms = async (username) => {
  try {
    const redis = getRedisClient();
    const rooms = await redis.sMembers(`user:rooms:${username}`);
    return rooms.map(r => JSON.parse(r));
  } catch (error) {
    console.error('Error getting user rooms:', error);
    return [];
  }
};

const setRoomLastMessage = async (roomId, message) => {
  try {
    const redis = getRedisClient();
    const msgData = JSON.stringify({
      message: message.message,
      username: message.username,
      timestamp: message.timestamp || new Date().toISOString()
    });
    await redis.set(`room:lastmsg:${roomId}`, msgData);
    await redis.expire(`room:lastmsg:${roomId}`, 86400); // 24 hours
    return true;
  } catch (error) {
    console.error('Error setting room last message:', error);
    return false;
  }
};

const getRoomLastMessage = async (roomId) => {
  try {
    const redis = getRedisClient();
    const msg = await redis.get(`room:lastmsg:${roomId}`);
    return msg ? JSON.parse(msg) : null;
  } catch (error) {
    console.error('Error getting room last message:', error);
    return null;
  }
};

const addUserDM = async (username, targetUsername) => {
  try {
    const redis = getRedisClient();
    const dmData = JSON.stringify({ username: targetUsername, addedAt: Date.now() });
    const result = await redis.sAdd(`user:dm:${username}`, dmData);
    console.log(`📩 addUserDM: Added ${targetUsername} to user:dm:${username} (result: ${result})`);
    return true;
  } catch (error) {
    console.error('Error adding user DM:', error);
    return false;
  }
};

const getUserDMs = async (username) => {
  try {
    const redis = getRedisClient();
    const dms = await redis.sMembers(`user:dm:${username}`);
    return dms.map(d => JSON.parse(d));
  } catch (error) {
    console.error('Error getting user DMs:', error);
    return [];
  }
};

const setDMLastMessage = async (userA, userB, message) => {
  try {
    const redis = getRedisClient();
    const key = [userA, userB].sort().join(':');
    const msgData = JSON.stringify({
      message: message.message,
      fromUsername: message.fromUsername,
      toUsername: message.toUsername,
      timestamp: message.timestamp || new Date().toISOString()
    });
    await redis.set(`dm:lastmsg:${key}`, msgData);
    await redis.expire(`dm:lastmsg:${key}`, 86400); // 24 hours
    return true;
  } catch (error) {
    console.error('Error setting DM last message:', error);
    return false;
  }
};

const getDMLastMessage = async (userA, userB) => {
  try {
    const redis = getRedisClient();
    const key = [userA, userB].sort().join(':');
    const msg = await redis.get(`dm:lastmsg:${key}`);
    return msg ? JSON.parse(msg) : null;
  } catch (error) {
    console.error('Error getting DM last message:', error);
    return null;
  }
};

const addRecentRoom = async (username, roomId, roomName) => {
  try {
    const redis = getRedisClient();
    const key = `user:recent:${username}`;
    const roomData = JSON.stringify({ roomId: roomId.toString(), roomName, visitedAt: Date.now() });
    const existing = await redis.lRange(key, 0, -1);
    for (const item of existing) {
      const data = JSON.parse(item);
      if (data.roomId === roomId.toString()) {
        await redis.lRem(key, 1, item);
        break;
      }
    }
    await redis.lPush(key, roomData);
    await redis.lTrim(key, 0, 9);
    return true;
  } catch (error) {
    console.error('Error adding recent room:', error);
    return false;
  }
};

const getRecentRooms = async (username) => {
  try {
    const client = getRedisClient();
    const rooms = await client.lRange(RECENT_ROOMS_KEY(username), 0, MAX_RECENT_ROOMS - 1);
    return rooms.map(r => JSON.parse(r));
  } catch (error) {
    console.error('Error getting recent rooms:', error);
    return [];
  }
};

const clearRecentRooms = async (username) => {
  try {
    const redis = getRedisClient();
    await redis.del(`user:recent:${username}`);
    return true;
  } catch (error) {
    console.error('Error clearing recent rooms:', error);
    return false;
  }
};

const getFavoriteRooms = async (username) => {
  try {
    const client = getRedisClient();
    const rooms = await client.sMembers(FAVORITE_ROOMS_KEY(username));
    return rooms;
  } catch (error) {
    console.error('Error getting favorite rooms:', error);
    return [];
  }
};

const addFavoriteRoom = async (username, roomId) => {
  try {
    const client = getRedisClient();
    await client.sAdd(FAVORITE_ROOMS_KEY(username), roomId.toString());
    return true;
  } catch (error) {
    console.error('Error adding favorite room:', error);
    return false;
  }
};

const removeFavoriteRoom = async (username, roomId) => {
  try {
    const client = getRedisClient();
    await client.sRem(FAVORITE_ROOMS_KEY(username), roomId.toString());
    return true;
  } catch (error) {
    console.error('Error removing favorite room:', error);
    return false;
  }
};

const getHotRooms = async (limit = 20) => {
  try {
    const client = getRedisClient();
    const rooms = await client.zRangeWithScores(HOT_ROOMS_KEY, 0, limit - 1, { REV: true });
    return rooms.map(r => ({ roomId: r.value, score: r.score }));
  } catch (error) {
    console.error('Error getting hot rooms:', error);
    return [];
  }
};

const updateHotRooms = async (roomId, increment = true) => {
  try {
    const client = getRedisClient();
    if (increment) {
      await client.zIncrBy(HOT_ROOMS_KEY, 1, roomId.toString());
    } else {
      await client.zIncrBy(HOT_ROOMS_KEY, -1, roomId.toString());
      const score = await client.zScore(HOT_ROOMS_KEY, roomId.toString());
      if (score <= 0) {
        await client.zRem(HOT_ROOMS_KEY, roomId.toString());
      }
    }
    return true;
  } catch (error) {
    console.error('Error updating hot rooms:', error);
    return false;
  }
};

const incrementRoomActive = async (roomId) => {
  try {
    const client = getRedisClient();
    await client.incr(ROOM_ACTIVE_KEY(roomId));
    await updateHotRooms(roomId, true);
    return true;
  } catch (error) {
    console.error('Error incrementing room active:', error);
    return false;
  }
};

const decrementRoomActive = async (roomId) => {
  try {
    const client = getRedisClient();
    const count = await client.decr(ROOM_ACTIVE_KEY(roomId));
    if (count < 0) {
      await client.set(ROOM_ACTIVE_KEY(roomId), '0');
    }
    await updateHotRooms(roomId, false);
    return true;
  } catch (error) {
    console.error('Error decrementing room active:', error);
    return false;
  }
};

const clearUserRooms = async (username) => {
  try {
    const redis = getRedisClient();
    await redis.del(`user:rooms:${username}`);
    return true;
  } catch (error) {
    console.error('Error clearing user rooms:', error);
    return false;
  }
};

const getUserActiveRooms = async (userId) => {
  try {
    const redis = getRedisClient();
    const key = `user:${userId}:activeRooms`;
    const rooms = await redis.sMembers(key);
    return rooms || [];
  } catch (error) {
    console.error('Error getting user active rooms:', error);
    return [];
  }
};

const addUserActiveRoom = async (userId, roomId) => {
  try {
    const redis = getRedisClient();
    const key = `user:${userId}:activeRooms`;
    await redis.sAdd(key, roomId.toString());
    // Set TTL of 24 hours as safety cleanup
    await redis.expire(key, 86400);
    return true;
  } catch (error) {
    console.error('Error adding user active room:', error);
    return false;
  }
};

const removeUserActiveRoom = async (userId, roomId) => {
  try {
    const redis = getRedisClient();
    const key = `user:${userId}:activeRooms`;
    await redis.sRem(key, roomId.toString());
    return true;
  } catch (error) {
    console.error('Error removing user active room:', error);
    return false;
  }
};

const clearUserActiveRooms = async (userId) => {
  try {
    const redis = getRedisClient();
    const key = `user:${userId}:activeRooms`;
    await redis.del(key);
    return true;
  } catch (error) {
    console.error('Error clearing user active rooms:', error);
    return false;
  }
};

module.exports = {
  setPresence,
  getPresence,
  removePresence,
  refreshOnlinePresence,
  setSession,
  getSession,
  removeSession,
  getRoomMembers,
  addRoomMember,
  removeRoomMember,
  setRoomUsers,
  getRoomUsers,
  setUserRoom,
  getUserRoom,
  removeUserRoom,
  setFlood,
  checkFlood,
  clearFlood,
  setTempKick,
  isTempKicked,
  clearTempKick,
  addUserRoom,
  removeUserRoom,
  getUserRooms,
  setRoomLastMessage,
  getRoomLastMessage,
  addUserDM,
  getUserDMs,
  setDMLastMessage,
  getDMLastMessage,
  addRecentRoom,
  getRecentRooms,
  clearRecentRooms,
  getFavoriteRooms,
  addFavoriteRoom,
  removeFavoriteRoom,
  getHotRooms,
  updateHotRooms,
  incrementRoomActive,
  decrementRoomActive,
  addUserToRoom,
  removeUserFromRoom,
  getRoomUsersList,
  getRoomParticipants,
  getRoomParticipantsWithNames,
  addRoomParticipant,
  removeRoomParticipant,
  getUserCurrentRoom,
  getRoomParticipantCount,
  clearUserRooms,
  getUserActiveRooms,
  addUserActiveRoom,
  removeUserActiveRoom,
  clearUserActiveRooms,
  DEFAULT_TTL,
  ONLINE_PRESENCE_TTL
};