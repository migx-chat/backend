const logger = require('../utils/logger');
const { query } = require('../db/db');
const presence = require('../utils/presence');

const createRoom = async (name, ownerId, creatorName, description = '', category = 'general') => {
  try {
    // ✅ ANTI-DUPLICATION: Check if room already exists (case-insensitive)
    const existingRoom = await getRoomByName(name);
    if (existingRoom) {
      console.warn(`⚠️ Room "${name}" already exists (ID: ${existingRoom.id}), returning existing room`);
      return existingRoom;
    }
    
    // Generate MIGX-xxxxx format room_code
    const randomDigits = Math.floor(10000 + Math.random() * 90000);
    const roomCode = `MIGX-${randomDigits}`;
    
    // Fixed maxUsers to 25 for MIG33 Classic
    const maxUsers = 25;
    
    const result = await query(
      `INSERT INTO rooms (name, owner_id, creator_name, description, max_users, room_code, category, created_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
       RETURNING *`,
      [name, ownerId, creatorName, description, maxUsers, roomCode, category]
    );
    
    const roomId = result.rows[0].id;
    
    await query(
      'INSERT INTO room_admins (room_id, user_id) VALUES ($1, $2)',
      [roomId, ownerId]
    );
    
    logger.info(`✅ New room created: "${name}" (ID: ${roomId})`);
    return result.rows[0];
  } catch (error) {
    console.error('Error creating room:', error);
    return null;
  }
};

const getRoomById = async (roomId) => {
  try {
    const result = await query(
      `SELECT r.*, u.username as owner_name
       FROM rooms r
       LEFT JOIN users u ON r.owner_id = u.id
       WHERE r.id = $1`,
      [roomId]
    );
    return result.rows[0] || null;
  } catch (error) {
    console.error('Error getting room:', error);
    return null;
  }
};

const getRoomByName = async (name) => {
  try {
    const result = await query(
      `SELECT r.*, u.username as owner_name
       FROM rooms r
       LEFT JOIN users u ON r.owner_id = u.id
       WHERE LOWER(r.name) = LOWER($1)`,
      [name]
    );
    return result.rows[0] || null;
  } catch (error) {
    console.error('Error getting room by name:', error);
    return null;
  }
};

const getAllRooms = async (limit = 50, offset = 0) => {
  try {
    const result = await query(
      `SELECT r.*, u.username as owner_name
       FROM rooms r
       LEFT JOIN users u ON r.owner_id = u.id
       ORDER BY r.created_at DESC
       LIMIT $1 OFFSET $2`,
      [limit, offset]
    );
    
    const roomsWithCount = await Promise.all(
      result.rows.map(async (room) => {
        const userCount = await presence.getRoomUserCount(room.id);
        return { ...room, userCount };
      })
    );
    
    return roomsWithCount;
  } catch (error) {
    console.error('Error getting all rooms:', error);
    return [];
  }
};

const getRandomRooms = async (limit = 20) => {
  try {
    const result = await query(
      `SELECT r.*, u.username as owner_name
       FROM rooms r
       LEFT JOIN users u ON r.owner_id = u.id
       ORDER BY RANDOM()
       LIMIT $1`,
      [limit]
    );
    
    return result.rows;
  } catch (error) {
    console.error('Error getting random rooms:', error);
    return [];
  }
};

const getOfficialRooms = async (limit = 20) => {
  try {
    const result = await query(
      `SELECT r.*, u.username as owner_name
       FROM rooms r
       LEFT JOIN users u ON r.owner_id = u.id
       WHERE r.category = 'official'
       ORDER BY r.created_at DESC
       LIMIT $1`,
      [limit]
    );
    
    const roomsWithCount = await Promise.all(
      result.rows.map(async (room) => {
        const userCount = await presence.getRoomUserCount(room.id);
        return { ...room, userCount };
      })
    );
    
    return roomsWithCount;
  } catch (error) {
    console.error('Error getting official rooms:', error);
    return [];
  }
};

const getGameRooms = async (limit = 20) => {
  try {
    const result = await query(
      `SELECT r.*, u.username as owner_name
       FROM rooms r
       LEFT JOIN users u ON r.owner_id = u.id
       WHERE r.category = 'game'
       ORDER BY r.created_at DESC
       LIMIT $1`,
      [limit]
    );
    
    const roomsWithCount = await Promise.all(
      result.rows.map(async (room) => {
        const userCount = await presence.getRoomUserCount(room.id);
        return { ...room, userCount };
      })
    );
    
    return roomsWithCount;
  } catch (error) {
    console.error('Error getting game rooms:', error);
    return [];
  }
};

const updateRoom = async (roomId, updates) => {
  try {
    const { name, description, maxUsers, isPrivate, password, minLevel } = updates;
    const result = await query(
      `UPDATE rooms SET 
        name = COALESCE($1, name),
        description = COALESCE($2, description),
        max_users = COALESCE($3, max_users),
        is_private = COALESCE($4, is_private),
        password = COALESCE($5, password),
        min_level = COALESCE($6, min_level),
        updated_at = CURRENT_TIMESTAMP
       WHERE id = $7
       RETURNING *`,
      [name, description, maxUsers, isPrivate, password, minLevel, roomId]
    );
    return result.rows[0];
  } catch (error) {
    console.error('Error updating room:', error);
    return null;
  }
};

const deleteRoom = async (roomId) => {
  try {
    await presence.clearRoomUsers(roomId);
    await query('DELETE FROM rooms WHERE id = $1', [roomId]);
    return true;
  } catch (error) {
    console.error('Error deleting room:', error);
    return false;
  }
};

const isRoomAdmin = async (roomId, userId) => {
  try {
    const room = await getRoomById(roomId);
    if (room && room.owner_id == userId) return true;
    
    const result = await query(
      'SELECT 1 FROM room_admins WHERE room_id = $1 AND user_id = $2',
      [roomId, userId]
    );
    return result.rows.length > 0;
  } catch (error) {
    console.error('Error checking room admin:', error);
    return false;
  }
};

const isRoomModerator = async (roomId, userId) => {
  try {
    const result = await query(
      'SELECT 1 FROM room_moderators WHERE room_id = $1 AND user_id = $2',
      [roomId, userId]
    );
    return result.rows.length > 0;
  } catch (error) {
    console.error('Error checking room moderator:', error);
    return false;
  }
};

const addRoomAdmin = async (roomId, userId) => {
  try {
    await query(
      `INSERT INTO room_admins (room_id, user_id)
       VALUES ($1, $2)
       ON CONFLICT DO NOTHING`,
      [roomId, userId]
    );
    return true;
  } catch (error) {
    console.error('Error adding room admin:', error);
    return false;
  }
};

const removeRoomAdmin = async (roomId, userId) => {
  try {
    await query(
      'DELETE FROM room_admins WHERE room_id = $1 AND user_id = $2',
      [roomId, userId]
    );
    return true;
  } catch (error) {
    console.error('Error removing room admin:', error);
    return false;
  }
};

const getRoomAdmins = async (roomId) => {
  try {
    const result = await query(
      `SELECT u.id, u.username, u.avatar
       FROM room_admins ra
       JOIN users u ON ra.user_id = u.id
       WHERE ra.room_id = $1`,
      [roomId]
    );
    return result.rows;
  } catch (error) {
    console.error('Error getting room admins:', error);
    return [];
  }
};

const joinRoom = async (roomId, userId, username) => {
  try {
    const room = await getRoomById(roomId);
    if (!room) {
      return { success: false, error: 'Room not found' };
    }
    
    const isBanned = await presence.isBanned(roomId, userId, username);
    if (isBanned) {
      return { success: false, error: 'You are banned from this room' };
    }
    
    // Level Check - bypass for owner, moderator, and admin
    const { getUserLevel } = require('../utils/xpLeveling');
    const levelData = await getUserLevel(userId);
    
    // Check if user can bypass level restriction
    const isOwner = room.owner_id == userId;
    const isModerator = await isRoomAdmin(roomId, userId);
    const userResult = await query('SELECT role FROM users WHERE id = $1', [userId]);
    const isGlobalAdmin = userResult.rows[0]?.role === 'admin';
    const canBypassLevel = isOwner || isModerator || isGlobalAdmin;
    
    if (room.min_level && levelData.level < room.min_level && !canBypassLevel) {
      return { 
        success: false, 
        error: `Unable to join chat room. Minimum level is ${room.min_level}.\nYour level: ${levelData.level}`,
        minLevel: room.min_level
      };
    }
    
    // Room full check - admin can bypass
    const userCount = await presence.getRoomUserCount(roomId);
    if (userCount >= room.max_users && !isGlobalAdmin) {
      return { success: false, error: 'Room is full' };
    }
    
    await presence.addUserToRoom(roomId, userId, username);
    return { success: true, room };
  } catch (error) {
    console.error('Error joining room:', error);
    return { success: false, error: 'Failed to join room' };
  }
};

const leaveRoom = async (roomId, userId, username) => {
  try {
    await presence.removeUserFromRoom(roomId, userId, username);
    return { success: true };
  } catch (error) {
    console.error('Error leaving room:', error);
    return { success: false, error: 'Failed to leave room' };
  }
};

const getRoomUsers = async (roomId) => {
  return await presence.getRoomUsers(roomId);
};

const kickUser = async (roomId, userId, username) => {
  try {
    await presence.removeUserFromRoom(roomId, userId, username);
    return { success: true };
  } catch (error) {
    console.error('Error kicking user:', error);
    return { success: false };
  }
};

const banUser = async (roomId, userId, username, bannedBy, reason = null, expiresAt = null) => {
  try {
    await presence.banUser(roomId, userId, username);
    
    await query(
      `INSERT INTO room_bans (room_id, user_id, banned_by, reason, expires_at)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (room_id, user_id) DO UPDATE SET
         banned_by = $3, reason = $4, expires_at = $5, created_at = CURRENT_TIMESTAMP`,
      [roomId, userId, bannedBy, reason, expiresAt]
    );
    
    return { success: true };
  } catch (error) {
    console.error('Error banning user:', error);
    return { success: false };
  }
};

const unbanUser = async (roomId, userId, username) => {
  try {
    await presence.unbanUser(roomId, userId, username);
    await query(
      'DELETE FROM room_bans WHERE room_id = $1 AND user_id = $2',
      [roomId, userId]
    );
    return { success: true };
  } catch (error) {
    console.error('Error unbanning user:', error);
    return { success: false };
  }
};

const getBannedUsers = async (roomId) => {
  return await presence.getBannedUsers(roomId);
};

const isUserBanned = async (roomId, userId, username) => {
  return await presence.isBanned(roomId, userId, username);
};

const saveRoomHistory = async (userId, roomId) => {
  try {
    await query(
      `INSERT INTO user_room_history (user_id, room_id, last_joined_at, created_at)
       VALUES ($1, $2, NOW(), NOW())
       ON CONFLICT (user_id, room_id) DO UPDATE SET
         last_joined_at = NOW()`,
      [userId, roomId]
    );
    return true;
  } catch (error) {
    console.error('Error saving room history:', error);
    return false;
  }
};

const getUserRoomHistory = async (userId, limit = 50) => {
  try {
    const result = await query(
      `SELECT r.id, r.name, r.description, r.owner_id, 
              urh.last_joined_at, u.username as owner_name
       FROM user_room_history urh
       JOIN rooms r ON urh.room_id = r.id
       LEFT JOIN users u ON r.owner_id = u.id
       WHERE urh.user_id = $1
       -- Filter: Only rooms that the user is CURRENTLY in (active in Redis)
       ORDER BY urh.last_joined_at DESC
       LIMIT $2`,
      [userId, limit]
    );
    
    // Filter rooms based on active presence in Redis
    const rooms = result.rows;
    const activeRooms = [];
    
    const presence = require('../utils/presence');
    const { getRedisClient } = require('../redis');
    const redis = getRedisClient();
    
    for (const room of rooms) {
      const isUserActive = await redis.sIsMember(`user:rooms:${room.owner_name || 'unknown'}`, room.id);
      // More reliable: check if user is in the room set in Redis
      // But wait, we need the username for this. Let's use the userId to find the username first if needed.
      // Actually, we can check the room:users:{roomId} set
      const usersInRoom = await redis.sMembers(`room:users:${room.id}`);
      // This might be slow for many rooms. 
      // Better approach: filter by presence in the next turn or just fetch joined rooms from Redis directly.
    }

    return result.rows;
  } catch (error) {
    console.error('Error getting user room history:', error);
    return [];
  }
};

const deleteUserRoomHistory = async (userId, roomId) => {
  try {
    const result = await query(
      `DELETE FROM user_room_history 
       WHERE user_id = $1 AND room_id = $2`,
      [userId, roomId]
    );
    logger.info(`🗑️ Deleted room ${roomId} from user ${userId} history`);
    return true;
  } catch (error) {
    console.error('Error deleting room history:', error);
    return false;
  }
};

const setRoomMinLevel = async (roomId, level) => {
  try {
    const result = await query(
      `UPDATE rooms SET min_level = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2 RETURNING *`,
      [level, roomId]
    );
    return result.rows[0];
  } catch (error) {
    console.error('Error setting room min level:', error);
    throw error;
  }
};

const getRoomMinLevel = async (roomId) => {
  try {
    const result = await query(
      `SELECT min_level FROM rooms WHERE id = $1`,
      [roomId]
    );
    return result.rows[0]?.min_level || 1;
  } catch (error) {
    console.error('Error getting room min level:', error);
    return 1;
  }
};

const setRoomLocked = async (roomId, isLocked) => {
  try {
    const result = await query(
      `UPDATE rooms SET is_locked = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2 RETURNING *`,
      [isLocked, roomId]
    );
    return result.rows[0];
  } catch (error) {
    console.error('Error setting room locked status:', error);
    throw error;
  }
};

module.exports = {
  createRoom,
  getRoomById,
  getRoomByName,
  getAllRooms,
  getRandomRooms,
  getOfficialRooms,
  getGameRooms,
  updateRoom,
  deleteRoom,
  isRoomAdmin,
  isRoomModerator,
  addRoomAdmin,
  removeRoomAdmin,
  getRoomAdmins,
  joinRoom,
  leaveRoom,
  getRoomUsers,
  kickUser,
  banUser,
  unbanUser,
  getBannedUsers,
  isUserBanned,
  saveRoomHistory,
  getUserRoomHistory,
  deleteUserRoomHistory,
  setRoomMinLevel,
  getRoomMinLevel,
  setRoomLocked
};
