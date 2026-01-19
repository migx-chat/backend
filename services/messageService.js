const { query } = require('../db/db');
const { generateMessageId } = require('../utils/idGenerator');

const MAX_MESSAGES_PER_ROOM = 242; // Limit like Miggi

const saveMessage = async (roomId, userId, username, message, messageType = 'chat', clientMsgId = null) => {
  try {
    const result = await query(
      `INSERT INTO messages (room_id, user_id, username, message, message_type, client_msg_id)
       VALUES ($1, $2, $3, $4, $5, $6)
       RETURNING id, room_id, user_id, username, message, message_type, client_msg_id, created_at`,
      [roomId, userId, username, message, messageType, clientMsgId]
    );
    
    // Cleanup old messages if exceeds limit (keep only latest 242)
    await cleanupOldMessages(roomId);
    
    return result.rows[0];
  } catch (error) {
    console.error('Error saving message:', error);
    return null;
  }
};

// Delete old messages if room has more than MAX_MESSAGES_PER_ROOM
const cleanupOldMessages = async (roomId) => {
  try {
    await query(
      `DELETE FROM messages 
       WHERE room_id = $1 
       AND id NOT IN (
         SELECT id FROM messages 
         WHERE room_id = $1 
         ORDER BY created_at DESC 
         LIMIT $2
       )`,
      [roomId, MAX_MESSAGES_PER_ROOM]
    );
  } catch (error) {
    console.error('Error cleaning up old messages:', error);
  }
};

const getMessages = async (roomId, limit = 50, offset = 0) => {
  try {
    const result = await query(
      `SELECT m.id, m.room_id, m.user_id, m.username, m.message, m.message_type, m.client_msg_id, m.created_at,
              u.avatar, u.role, u.username_color, u.username_color_expiry,
              COALESCE(ul.level, 1) as user_level
       FROM messages m
       LEFT JOIN users u ON m.user_id = u.id
       LEFT JOIN user_levels ul ON m.user_id = ul.user_id
       WHERE m.room_id = $1
       ORDER BY m.created_at DESC
       LIMIT $2 OFFSET $3`,
      [roomId, limit, offset]
    );
    return result.rows.reverse();
  } catch (error) {
    console.error('Error getting messages:', error);
    return [];
  }
};

// Get message count for a room (for unread calculation)
const getMessageCount = async (roomId) => {
  try {
    const result = await query(
      'SELECT COUNT(*) as count FROM messages WHERE room_id = $1',
      [roomId]
    );
    return parseInt(result.rows[0]?.count || 0);
  } catch (error) {
    console.error('Error getting message count:', error);
    return 0;
  }
};

// Get messages after a certain timestamp (for unread)
const getMessagesAfter = async (roomId, afterTimestamp, limit = 242) => {
  try {
    const result = await query(
      `SELECT m.*, u.avatar, u.role, u.username_color, u.username_color_expiry,
              COALESCE(ul.level, 1) as user_level
       FROM messages m
       LEFT JOIN users u ON m.user_id = u.id
       LEFT JOIN user_levels ul ON m.user_id = ul.user_id
       WHERE m.room_id = $1 AND m.created_at > $2
       ORDER BY m.created_at ASC
       LIMIT $3`,
      [roomId, afterTimestamp, limit]
    );
    return result.rows;
  } catch (error) {
    console.error('Error getting messages after timestamp:', error);
    return [];
  }
};

const getMessageById = async (messageId) => {
  try {
    const result = await query(
      'SELECT * FROM messages WHERE id = $1',
      [messageId]
    );
    return result.rows[0] || null;
  } catch (error) {
    console.error('Error getting message:', error);
    return null;
  }
};

const deleteMessage = async (messageId) => {
  try {
    await query('DELETE FROM messages WHERE id = $1', [messageId]);
    return true;
  } catch (error) {
    console.error('Error deleting message:', error);
    return false;
  }
};

const clearRoomMessages = async (roomId) => {
  try {
    await query('DELETE FROM messages WHERE room_id = $1', [roomId]);
    return true;
  } catch (error) {
    console.error('Error clearing room messages:', error);
    return false;
  }
};

const savePrivateMessage = async (fromUserId, toUserId, fromUsername, toUsername, message) => {
  try {
    const result = await query(
      `INSERT INTO private_messages (from_user_id, to_user_id, from_username, to_username, message)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING *`,
      [fromUserId, toUserId, fromUsername, toUsername, message]
    );
    return result.rows[0];
  } catch (error) {
    console.error('Error saving private message:', error);
    return null;
  }
};

const getPrivateMessages = async (userId1, userId2, limit = 50, offset = 0) => {
  try {
    const result = await query(
      `SELECT pm.*, u.role as from_role, u.avatar as from_avatar
       FROM private_messages pm
       LEFT JOIN users u ON pm.from_user_id = u.id
       WHERE (pm.from_user_id = $1 AND pm.to_user_id = $2)
          OR (pm.from_user_id = $2 AND pm.to_user_id = $1)
       ORDER BY pm.created_at DESC
       LIMIT $3 OFFSET $4`,
      [userId1, userId2, limit, offset]
    );
    return result.rows.reverse();
  } catch (error) {
    console.error('Error getting private messages:', error);
    return [];
  }
};

const getUnreadMessages = async (userId) => {
  try {
    const result = await query(
      `SELECT pm.*, u.avatar as from_avatar
       FROM private_messages pm
       LEFT JOIN users u ON pm.from_user_id = u.id
       WHERE pm.to_user_id = $1 AND pm.is_read = FALSE
       ORDER BY pm.created_at DESC`,
      [userId]
    );
    return result.rows;
  } catch (error) {
    console.error('Error getting unread messages:', error);
    return [];
  }
};

const markMessagesAsRead = async (userId, fromUserId) => {
  try {
    await query(
      `UPDATE private_messages SET is_read = TRUE
       WHERE to_user_id = $1 AND from_user_id = $2 AND is_read = FALSE`,
      [userId, fromUserId]
    );
    return true;
  } catch (error) {
    console.error('Error marking messages as read:', error);
    return false;
  }
};

const getRecentConversations = async (userId, limit = 20) => {
  try {
    const result = await query(
      `SELECT DISTINCT ON (other_user_id)
         CASE 
           WHEN from_user_id = $1 THEN to_user_id
           ELSE from_user_id
         END as other_user_id,
         CASE 
           WHEN from_user_id = $1 THEN to_username
           ELSE from_username
         END as other_username,
         message,
         created_at,
         is_read
       FROM private_messages
       WHERE from_user_id = $1 OR to_user_id = $1
       ORDER BY other_user_id, created_at DESC
       LIMIT $2`,
      [userId, limit]
    );
    return result.rows;
  } catch (error) {
    console.error('Error getting recent conversations:', error);
    return [];
  }
};

const createSystemMessage = (roomId, message) => {
  return {
    id: generateMessageId(),
    roomId,
    username: 'System',
    message,
    messageType: 'system',
    createdAt: new Date().toISOString()
  };
};

const createNoticeMessage = (roomId, message) => {
  return {
    id: generateMessageId(),
    roomId,
    username: '',
    message,
    messageType: 'notice',
    createdAt: new Date().toISOString()
  };
};

module.exports = {
  saveMessage,
  getMessages,
  getMessageCount,
  getMessagesAfter,
  getMessageById,
  deleteMessage,
  clearRoomMessages,
  savePrivateMessage,
  getPrivateMessages,
  getUnreadMessages,
  markMessagesAsRead,
  getRecentConversations,
  createSystemMessage,
  createNoticeMessage,
  MAX_MESSAGES_PER_ROOM
};
