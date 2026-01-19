const { query } = require('../db/db');
const presence = require('../utils/presence');

const banUserFromRoom = async (roomId, userId, username, bannedBy, reason = null, duration = null) => {
  try {
    let expiresAt = null;
    if (duration) {
      expiresAt = new Date(Date.now() + duration * 1000);
    }
    
    await presence.banUser(roomId, userId, username);
    
    await query(
      `INSERT INTO room_bans (room_id, user_id, banned_by, reason, expires_at)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (room_id, user_id) DO UPDATE SET
         banned_by = $3, reason = $4, expires_at = $5, created_at = CURRENT_TIMESTAMP`,
      [roomId, userId, bannedBy, reason, expiresAt]
    );
    
    return { success: true, expiresAt };
  } catch (error) {
    console.error('Error banning user from room:', error);
    return { success: false, error: 'Failed to ban user' };
  }
};

const unbanUserFromRoom = async (roomId, userId, username) => {
  try {
    await presence.unbanUser(roomId, userId, username);
    await query(
      'DELETE FROM room_bans WHERE room_id = $1 AND user_id = $2',
      [roomId, userId]
    );
    return { success: true };
  } catch (error) {
    console.error('Error unbanning user from room:', error);
    return { success: false, error: 'Failed to unban user' };
  }
};

const isUserBannedFromRoom = async (roomId, userId, username) => {
  try {
    const redisCheck = await presence.isBanned(roomId, userId, username);
    if (redisCheck) return true;
    
    const result = await query(
      `SELECT * FROM room_bans 
       WHERE room_id = $1 AND user_id = $2 
       AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)`,
      [roomId, userId]
    );
    
    if (result.rows.length > 0) {
      await presence.banUser(roomId, userId, username);
      return true;
    }
    
    return false;
  } catch (error) {
    console.error('Error checking ban status:', error);
    return false;
  }
};

const getRoomBannedUsers = async (roomId) => {
  try {
    const result = await query(
      `SELECT rb.*, u.username, u.avatar, bu.username as banned_by_username
       FROM room_bans rb
       JOIN users u ON rb.user_id = u.id
       LEFT JOIN users bu ON rb.banned_by = bu.id
       WHERE rb.room_id = $1
       AND (rb.expires_at IS NULL OR rb.expires_at > CURRENT_TIMESTAMP)`,
      [roomId]
    );
    return result.rows;
  } catch (error) {
    console.error('Error getting banned users:', error);
    return [];
  }
};

const cleanupExpiredBans = async () => {
  try {
    const result = await query(
      `DELETE FROM room_bans 
       WHERE expires_at IS NOT NULL AND expires_at <= CURRENT_TIMESTAMP
       RETURNING room_id, user_id`
    );
    
    for (const ban of result.rows) {
      const userResult = await query('SELECT username FROM users WHERE id = $1', [ban.user_id]);
      if (userResult.rows[0]) {
        await presence.unbanUser(ban.room_id, ban.user_id, userResult.rows[0].username);
      }
    }
    
    return result.rows.length;
  } catch (error) {
    console.error('Error cleaning up expired bans:', error);
    return 0;
  }
};

const getUserBans = async (userId) => {
  try {
    const result = await query(
      `SELECT rb.*, r.name as room_name
       FROM room_bans rb
       JOIN rooms r ON rb.room_id = r.id
       WHERE rb.user_id = $1
       AND (rb.expires_at IS NULL OR rb.expires_at > CURRENT_TIMESTAMP)`,
      [userId]
    );
    return result.rows;
  } catch (error) {
    console.error('Error getting user bans:', error);
    return [];
  }
};

const kickUserFromRoom = async (roomId, userId, username) => {
  try {
    await presence.removeUserFromRoom(roomId, userId, username);
    return { success: true };
  } catch (error) {
    console.error('Error kicking user:', error);
    return { success: false, error: 'Failed to kick user' };
  }
};

module.exports = {
  banUserFromRoom,
  unbanUserFromRoom,
  isUserBannedFromRoom,
  getRoomBannedUsers,
  cleanupExpiredBans,
  getUserBans,
  kickUserFromRoom
};
