const { query } = require('../db/db');

const addModerator = async (roomId, userId) => {
  try {
    const result = await query(
      `INSERT INTO room_moderators (room_id, user_id) 
       VALUES ($1, $2) 
       ON CONFLICT (room_id, user_id) DO NOTHING
       RETURNING id, room_id, user_id`,
      [roomId, userId]
    );
    return result.rows[0] || null;
  } catch (error) {
    console.error('Error adding moderator:', error);
    return null;
  }
};

const removeModerator = async (roomId, userId) => {
  try {
    await query(
      `DELETE FROM room_moderators WHERE room_id = $1 AND user_id = $2`,
      [roomId, userId]
    );
    return true;
  } catch (error) {
    console.error('Error removing moderator:', error);
    return false;
  }
};

const isModerator = async (roomId, userId) => {
  try {
    const result = await query(
      `SELECT id FROM room_moderators WHERE room_id = $1 AND user_id = $2`,
      [roomId, userId]
    );
    return result.rows.length > 0;
  } catch (error) {
    console.error('Error checking moderator:', error);
    return false;
  }
};

const getRoomModerators = async (roomId) => {
  try {
    const result = await query(
      `SELECT u.id, u.username FROM room_moderators rm
       JOIN users u ON rm.user_id = u.id
       WHERE rm.room_id = $1
       ORDER BY rm.added_at ASC`,
      [roomId]
    );
    return result.rows;
  } catch (error) {
    console.error('Error getting room moderators:', error);
    return [];
  }
};

module.exports = {
  addModerator,
  removeModerator,
  isModerator,
  getRoomModerators
};
