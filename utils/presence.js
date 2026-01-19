const { getRedisClient } = require('../redis');
const client = getRedisClient();

const ROOM_USERS_KEY = (roomId) => `room:users:${roomId}`;
const USER_ROOM_KEY = (username) => `user:room:${username}`;
const FLOOD_KEY = (username) => `flood:${username}`;
const ROOM_KICK_KEY = (username) => `room:kick:${username}`;
const ROOM_BANNED_KEY = (roomId) => `room:${roomId}:banned`;
const USER_SOCKET_KEY = (userId) => `user:${userId}:socket`;
const PRESENCE_KEY = (username) => `presence:${username}`;
const SESSION_KEY = (username) => `session:${username}`;
const ROOM_MEMBERS_KEY = (roomId) => `room:members:${roomId}`;
const ROOM_INFO_KEY = (roomId) => `room:info:${roomId}`;

const DEFAULT_TTL = 300;
const ONLINE_PRESENCE_TTL = 180;

const addUserToRoomDetailed = async (roomId, userId, username) => {
  try {
    const userData = JSON.stringify({ id: userId, username, joinedAt: Date.now() });
    await client.sAdd(ROOM_USERS_KEY(roomId), userData);
    await client.expire(ROOM_USERS_KEY(roomId), DEFAULT_TTL);
    await client.set(USER_ROOM_KEY(username), roomId.toString());
    await client.expire(USER_ROOM_KEY(username), DEFAULT_TTL);
    return true;
  } catch (error) {
    console.error('Error adding user to room:', error);
    return false;
  }
};

const removeUserFromRoom = async (roomId, userId, username) => {
  try {
    const members = await client.sMembers(ROOM_USERS_KEY(roomId));
    for (const member of members) {
      const data = JSON.parse(member);
      if (data.id == userId || data.username === username) {
        await client.sRem(ROOM_USERS_KEY(roomId), member);
        break;
      }
    }
    await client.del(USER_ROOM_KEY(username));
    return true;
  } catch (error) {
    console.error('Error removing user from room:', error);
    return false;
  }
};

const getRoomUsers = async (roomId) => {
  try {
    const members = await client.sMembers(ROOM_USERS_KEY(roomId));
    return members.map((m) => {
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

const getRoomUserCount = async (roomId) => {
  try {
    return await client.sCard(ROOM_USERS_KEY(roomId));
  } catch (error) {
    console.error('Error getting room user count:', error);
    return 0;
  }
};

const getUserRoom = async (username) => {
  try {
    return await client.get(USER_ROOM_KEY(username));
  } catch (error) {
    console.error('Error getting user room:', error);
    return null;
  }
};

const getUserRooms = async (username) => {
  try {
    const roomId = await client.get(USER_ROOM_KEY(username));
    if (roomId) {
      return [roomId];
    }
    return [];
  } catch (error) {
    console.error('Error getting user rooms:', error);
    return [];
  }
};

const setFloodControl = async (username) => {
  try {
    await client.set(FLOOD_KEY(username), '1');
    await client.expire(FLOOD_KEY(username), DEFAULT_TTL);
    return true;
  } catch (error) {
    console.error('Error setting flood control:', error);
    return false;
  }
};

const checkFloodControl = async (username) => {
  try {
    const exists = await client.exists(FLOOD_KEY(username));
    return exists === 1;
  } catch (error) {
    console.error('Error checking flood control:', error);
    return false;
  }
};

const clearFloodControl = async (username) => {
  try {
    await client.del(FLOOD_KEY(username));
    return true;
  } catch (error) {
    console.error('Error clearing flood control:', error);
    return false;
  }
};

const setTempBan = async (username, roomId) => {
  try {
    await client.set(ROOM_KICK_KEY(username), roomId.toString());
    await client.expire(ROOM_KICK_KEY(username), DEFAULT_TTL);
    return true;
  } catch (error) {
    console.error('Error setting temp ban:', error);
    return false;
  }
};

const isTempBanned = async (username) => {
  try {
    const roomId = await client.get(ROOM_KICK_KEY(username));
    return roomId ? { banned: true, roomId } : { banned: false };
  } catch (error) {
    console.error('Error checking temp ban:', error);
    return { banned: false };
  }
};

const clearTempBan = async (username) => {
  try {
    await client.del(ROOM_KICK_KEY(username));
    return true;
  } catch (error) {
    console.error('Error clearing temp ban:', error);
    return false;
  }
};

const banUser = async (roomId, userId, username) => {
  try {
    const banData = JSON.stringify({ id: userId, username, bannedAt: Date.now() });
    await client.sAdd(ROOM_BANNED_KEY(roomId), banData);
    await removeUserFromRoom(roomId, userId, username);
    return true;
  } catch (error) {
    console.error('Error banning user:', error);
    return false;
  }
};

const unbanUser = async (roomId, userId, username) => {
  try {
    const members = await client.sMembers(ROOM_BANNED_KEY(roomId));
    for (const member of members) {
      const data = JSON.parse(member);
      if (data.id == userId || data.username === username) {
        await client.sRem(ROOM_BANNED_KEY(roomId), member);
        return true;
      }
    }
    return false;
  } catch (error) {
    console.error('Error unbanning user:', error);
    return false;
  }
};

const isBanned = async (roomId, userId, username) => {
  try {
    const members = await client.sMembers(ROOM_BANNED_KEY(roomId));
    for (const member of members) {
      const data = JSON.parse(member);
      if (data.id == userId || data.username === username) {
        return true;
      }
    }
    return false;
  } catch (error) {
    console.error('Error checking ban status:', error);
    return false;
  }
};

const getBannedUsers = async (roomId) => {
  try {
    const members = await client.sMembers(ROOM_BANNED_KEY(roomId));
    return members.map((m) => {
      try {
        return JSON.parse(m);
      } catch {
        return { username: m };
      }
    });
  } catch (error) {
    console.error('Error getting banned users:', error);
    return [];
  }
};

const setUserSocket = async (userId, socketId) => {
  try {
    await client.set(USER_SOCKET_KEY(userId), socketId);
    await client.expire(USER_SOCKET_KEY(userId), DEFAULT_TTL);
    return true;
  } catch (error) {
    console.error('Error setting user socket:', error);
    return false;
  }
};

const getUserSocket = async (userId) => {
  try {
    return await client.get(USER_SOCKET_KEY(userId));
  } catch (error) {
    console.error('Error getting user socket:', error);
    return null;
  }
};

const removeUserSocket = async (userId) => {
  try {
    await client.del(USER_SOCKET_KEY(userId));
    return true;
  } catch (error) {
    console.error('Error removing user socket:', error);
    return false;
  }
};

const clearRoomUsers = async (roomId) => {
  try {
    await client.del(ROOM_USERS_KEY(roomId));
    return true;
  } catch (error) {
    console.error('Error clearing room users:', error);
    return false;
  }
};

const setPresence = async (username, status) => {
  try {
    await client.set(PRESENCE_KEY(username), status);
    if (status === 'online') {
      await client.expire(PRESENCE_KEY(username), ONLINE_PRESENCE_TTL);
    } else if (status === 'away' || status === 'busy') {
      await client.persist(PRESENCE_KEY(username));
    } else if (status === 'offline') {
      await client.del(PRESENCE_KEY(username));
    }
    return true;
  } catch (error) {
    console.error('Error setting presence:', error);
    return false;
  }
};

const getPresence = async (username) => {
  try {
    const presence = await client.get(PRESENCE_KEY(username));
    return presence || 'offline';
  } catch (error) {
    console.error('Error getting presence:', error);
    return 'offline';
  }
};

const removePresence = async (username) => {
  try {
    await client.del(PRESENCE_KEY(username));
    return true;
  } catch (error) {
    console.error('Error removing presence:', error);
    return false;
  }
};

const refreshOnlinePresence = async (username) => {
  try {
    const currentPresence = await getPresence(username);
    if (currentPresence === 'online') {
      await client.expire(PRESENCE_KEY(username), ONLINE_PRESENCE_TTL);
    }
    return true;
  } catch (error) {
    console.error('Error refreshing online presence:', error);
    return false;
  }
};

const setSession = async (username, socketId) => {
  try {
    const existingSession = await client.get(SESSION_KEY(username));
    if (existingSession && existingSession !== socketId) {
      return { success: false, existingSocketId: existingSession };
    }
    await client.set(SESSION_KEY(username), socketId);
    await client.expire(SESSION_KEY(username), DEFAULT_TTL);
    return { success: true };
  } catch (error) {
    console.error('Error setting session:', error);
    return { success: false, error: 'Failed to set session' };
  }
};

const getSession = async (username) => {
  try {
    return await client.get(SESSION_KEY(username));
  } catch (error) {
    console.error('Error getting session:', error);
    return null;
  }
};

const removeSession = async (username) => {
  try {
    await client.del(SESSION_KEY(username));
    return true;
  } catch (error) {
    console.error('Error removing session:', error);
    return false;
  }
};

const addMemberToRoom = async (roomId, username) => {
  try {
    await client.sAdd(ROOM_MEMBERS_KEY(roomId), username);
    await client.expire(ROOM_MEMBERS_KEY(roomId), DEFAULT_TTL);
    return true;
  } catch (error) {
    console.error('Error adding member to room:', error);
    return false;
  }
};

const removeMemberFromRoom = async (roomId, username) => {
  try {
    await client.sRem(ROOM_MEMBERS_KEY(roomId), username);
    return true;
  } catch (error) {
    console.error('Error removing member from room:', error);
    return false;
  }
};

const getRoomMembers = async (roomId) => {
  try {
    return await client.sMembers(ROOM_MEMBERS_KEY(roomId));
  } catch (error) {
    console.error('Error getting room members:', error);
    return [];
  }
};

const getRoomMemberCount = async (roomId) => {
  try {
    return await client.sCard(ROOM_MEMBERS_KEY(roomId));
  } catch (error) {
    console.error('Error getting room member count:', error);
    return 0;
  }
};

const isMemberInRoom = async (roomId, username) => {
  try {
    return await client.sIsMember(ROOM_MEMBERS_KEY(roomId), username);
  } catch (error) {
    console.error('Error checking member in room:', error);
    return false;
  }
};

const setRoomInfo = async (roomId, roomData) => {
  try {
    await client.set(ROOM_INFO_KEY(roomId), JSON.stringify(roomData));
    await client.expire(ROOM_INFO_KEY(roomId), DEFAULT_TTL);
    return true;
  } catch (error) {
    console.error('Error setting room info:', error);
    return false;
  }
};

const getRoomInfo = async (roomId) => {
  try {
    const data = await client.get(ROOM_INFO_KEY(roomId));
    return data ? JSON.parse(data) : null;
  } catch (error) {
    console.error('Error getting room info:', error);
    return null;
  }
};

const removeRoomInfo = async (roomId) => {
  try {
    await client.del(ROOM_INFO_KEY(roomId));
    return true;
  } catch (error) {
    console.error('Error removing room info:', error);
    return false;
  }
};

module.exports = {
  addUserToRoomDetailed,
  removeUserFromRoom,
  getRoomUsers,
  getRoomUserCount,
  getUserRoom,
  getUserRooms,
  setFloodControl,
  checkFloodControl,
  clearFloodControl,
  setTempBan,
  isTempBanned,
  clearTempBan,
  banUser,
  unbanUser,
  isBanned,
  getBannedUsers,
  setUserSocket,
  getUserSocket,
  removeUserSocket,
  clearRoomUsers,
  setPresence,
  getPresence,
  removePresence,
  refreshOnlinePresence,
  setSession,
  getSession,
  removeSession,
  addMemberToRoom,
  removeMemberFromRoom,
  getRoomMembers,
  getRoomMemberCount,
  isMemberInRoom,
  setRoomInfo,
  getRoomInfo,
  removeRoomInfo,
  DEFAULT_TTL,
  ONLINE_PRESENCE_TTL
};
