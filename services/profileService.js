
const { query } = require('../db/db');
const path = require('path');
const fs = require('fs').promises;

// ==================== POSTS ====================

const createPost = async (userId, content, imageUrl = null) => {
  try {
    const result = await query(
      `INSERT INTO user_posts (user_id, content, image_url)
       VALUES ($1, $2, $3)
       RETURNING *`,
      [userId, content, imageUrl]
    );
    return result.rows[0];
  } catch (error) {
    console.error('Error creating post:', error);
    return null;
  }
};

const getUserPosts = async (userId, limit = 20, offset = 0) => {
  try {
    const result = await query(
      `SELECT p.*, u.username, u.avatar 
       FROM user_posts p
       JOIN users u ON p.user_id = u.id
       WHERE p.user_id = $1
       ORDER BY p.created_at DESC
       LIMIT $2 OFFSET $3`,
      [userId, limit, offset]
    );
    return result.rows;
  } catch (error) {
    console.error('Error getting user posts:', error);
    return [];
  }
};

const getPostCount = async (userId) => {
  try {
    const result = await query(
      'SELECT COUNT(*) as count FROM user_posts WHERE user_id = $1',
      [userId]
    );
    return parseInt(result.rows[0].count) || 0;
  } catch (error) {
    console.error('Error getting post count:', error);
    return 0;
  }
};

const deletePost = async (postId, userId) => {
  try {
    const result = await query(
      'DELETE FROM user_posts WHERE id = $1 AND user_id = $2 RETURNING *',
      [postId, userId]
    );
    return result.rows[0];
  } catch (error) {
    console.error('Error deleting post:', error);
    return null;
  }
};

// ==================== GIFTS ====================

const sendGift = async (senderId, receiverId, giftName, giftIcon, giftCost) => {
  try {
    const result = await query(
      `INSERT INTO user_gifts (sender_id, receiver_id, gift_name, gift_icon, gift_cost)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING *`,
      [senderId, receiverId, giftName, giftIcon, giftCost]
    );
    
    // Also update a total received counter if we wanted to persist it in the users table, 
    // but the current approach of counting from user_gifts is more accurate.
    // However, since the user says "save to db agar tidak hilang", let's ensure the table is robust.
    
    return result.rows[0];
  } catch (error) {
    console.error('Error sending gift:', error);
    return null;
  }
};

const getReceivedGifts = async (userId, limit = 20, offset = 0) => {
  try {
    const result = await query(
      `SELECT g.*, u.username as sender_username, u.avatar as sender_avatar
       FROM user_gifts g
       JOIN users u ON g.sender_id = u.id
       WHERE g.receiver_id = $1
       ORDER BY g.created_at DESC
       LIMIT $2 OFFSET $3`,
      [userId, limit, offset]
    );
    return result.rows;
  } catch (error) {
    console.error('Error getting received gifts:', error);
    return [];
  }
};

const getSentGifts = async (userId, limit = 20, offset = 0) => {
  try {
    const result = await query(
      `SELECT g.*, u.username as receiver_username, u.avatar as receiver_avatar
       FROM user_gifts g
       JOIN users u ON g.receiver_id = u.id
       WHERE g.sender_id = $1
       ORDER BY g.created_at DESC
       LIMIT $2 OFFSET $3`,
      [userId, limit, offset]
    );
    return result.rows;
  } catch (error) {
    console.error('Error getting sent gifts:', error);
    return [];
  }
};

const getGiftCount = async (userId) => {
  try {
    const result = await query(
      'SELECT COUNT(*) as count FROM user_gifts WHERE receiver_id = $1',
      [userId]
    );
    const count = parseInt(result.rows[0].count) || 0;
    return count;
  } catch (error) {
    console.error('Error getting gift count:', error);
    return 0;
  }
};

// ==================== FOOTPRINTS ====================

const getFootprintCount = async (userId) => {
  try {
    const result = await query(
      'SELECT COUNT(*) as count FROM profile_footprints WHERE profile_id = $1',
      [userId]
    );
    return parseInt(result.rows[0].count) || 0;
  } catch (error) {
    console.error('Error getting footprint count:', error);
    return 0;
  }
};

// ==================== FOLLOWS (with pending/accepted status) ====================

const followUser = async (followerId, followingId) => {
  try {
    if (followerId === followingId) {
      return { error: 'Cannot follow yourself' };
    }

    // Check if there's already a follow record
    const existing = await query(
      'SELECT * FROM user_follows WHERE follower_id = $1 AND following_id = $2',
      [followerId, followingId]
    );
    
    if (existing.rows.length > 0) {
      const status = existing.rows[0].status;
      if (status === 'accepted') {
        return { error: 'Already following this user' };
      } else if (status === 'pending') {
        return { error: 'Follow request already pending' };
      } else if (status === 'rejected') {
        // Re-send request if previously rejected
        const result = await query(
          `UPDATE user_follows SET status = 'pending', updated_at = CURRENT_TIMESTAMP 
           WHERE follower_id = $1 AND following_id = $2 RETURNING *`,
          [followerId, followingId]
        );
        return { ...result.rows[0], status: 'pending' };
      }
    }

    // Create new pending follow request
    const result = await query(
      `INSERT INTO user_follows (follower_id, following_id, status)
       VALUES ($1, $2, 'pending')
       RETURNING *`,
      [followerId, followingId]
    );
    
    return { ...result.rows[0], status: 'pending' };
  } catch (error) {
    console.error('Error following user:', error);
    return { error: 'Failed to follow user' };
  }
};

const acceptFollowRequest = async (userId, followerId) => {
  try {
    const result = await query(
      `UPDATE user_follows SET status = 'accepted', updated_at = CURRENT_TIMESTAMP 
       WHERE follower_id = $1 AND following_id = $2 AND status = 'pending'
       RETURNING *`,
      [followerId, userId]
    );
    
    if (result.rows.length === 0) {
      return { error: 'No pending follow request found' };
    }
    
    return result.rows[0];
  } catch (error) {
    console.error('Error accepting follow request:', error);
    return { error: 'Failed to accept follow request' };
  }
};

const rejectFollowRequest = async (userId, followerId) => {
  try {
    const result = await query(
      `UPDATE user_follows SET status = 'rejected', updated_at = CURRENT_TIMESTAMP 
       WHERE follower_id = $1 AND following_id = $2 AND status = 'pending'
       RETURNING *`,
      [followerId, userId]
    );
    
    if (result.rows.length === 0) {
      return { error: 'No pending follow request found' };
    }
    
    return result.rows[0];
  } catch (error) {
    console.error('Error rejecting follow request:', error);
    return { error: 'Failed to reject follow request' };
  }
};

const getPendingFollowRequests = async (userId, limit = 50, offset = 0) => {
  try {
    const result = await query(
      `SELECT u.id, u.username, u.avatar, u.status as user_status, uf.created_at as requested_at
       FROM user_follows uf
       JOIN users u ON uf.follower_id = u.id
       WHERE uf.following_id = $1 AND uf.status = 'pending'
       ORDER BY uf.created_at DESC
       LIMIT $2 OFFSET $3`,
      [userId, limit, offset]
    );
    return result.rows;
  } catch (error) {
    console.error('Error getting pending follow requests:', error);
    return [];
  }
};

const getPendingFollowRequestsCount = async (userId) => {
  try {
    const result = await query(
      `SELECT COUNT(*) as count FROM user_follows WHERE following_id = $1 AND status = 'pending'`,
      [userId]
    );
    return parseInt(result.rows[0].count) || 0;
  } catch (error) {
    console.error('Error getting pending follow requests count:', error);
    return 0;
  }
};

const unfollowUser = async (followerId, followingId) => {
  try {
    const result = await query(
      'DELETE FROM user_follows WHERE follower_id = $1 AND following_id = $2 RETURNING *',
      [followerId, followingId]
    );
    return result.rows[0];
  } catch (error) {
    console.error('Error unfollowing user:', error);
    return null;
  }
};

const getFollowers = async (userId, limit = 50, offset = 0) => {
  try {
    // Only return ACCEPTED followers
    const result = await query(
      `SELECT u.id, u.username, u.avatar, u.status, u.status_message, u.last_login_date, uf.created_at as followed_at
       FROM user_follows uf
       JOIN users u ON uf.follower_id = u.id
       WHERE uf.following_id = $1 AND uf.status = 'accepted'
       ORDER BY uf.created_at DESC
       LIMIT $2 OFFSET $3`,
      [userId, limit, offset]
    );
    return result.rows;
  } catch (error) {
    console.error('Error getting followers:', error);
    return [];
  }
};

const getFollowing = async (userId, limit = 50, offset = 0) => {
  try {
    // Only return ACCEPTED following
    const result = await query(
      `SELECT u.id, u.username, u.avatar, u.status, u.status_message, u.last_login_date, uf.created_at as followed_at
       FROM user_follows uf
       JOIN users u ON uf.following_id = u.id
       WHERE uf.follower_id = $1 AND uf.status = 'accepted'
       ORDER BY uf.created_at DESC
       LIMIT $2 OFFSET $3`,
      [userId, limit, offset]
    );
    return result.rows;
  } catch (error) {
    console.error('Error getting following:', error);
    return [];
  }
};

const getFollowersCount = async (userId) => {
  try {
    // Only count ACCEPTED followers
    const result = await query(
      `SELECT COUNT(*) as count FROM user_follows WHERE following_id = $1 AND status = 'accepted'`,
      [userId]
    );
    return parseInt(result.rows[0].count) || 0;
  } catch (error) {
    console.error('Error getting followers count:', error);
    return 0;
  }
};

const getFollowingCount = async (userId) => {
  try {
    // Only count ACCEPTED following
    const result = await query(
      `SELECT COUNT(*) as count FROM user_follows WHERE follower_id = $1 AND status = 'accepted'`,
      [userId]
    );
    return parseInt(result.rows[0].count) || 0;
  } catch (error) {
    console.error('Error getting following count:', error);
    return 0;
  }
};

const isFollowing = async (followerId, followingId) => {
  try {
    // Check if ACCEPTED follow exists
    const result = await query(
      `SELECT * FROM user_follows WHERE follower_id = $1 AND following_id = $2 AND status = 'accepted'`,
      [followerId, followingId]
    );
    return result.rows.length > 0;
  } catch (error) {
    console.error('Error checking follow status:', error);
    return false;
  }
};

const getFollowStatus = async (followerId, followingId) => {
  try {
    const result = await query(
      `SELECT status FROM user_follows WHERE follower_id = $1 AND following_id = $2`,
      [followerId, followingId]
    );
    if (result.rows.length === 0) {
      return null; // Not following
    }
    return result.rows[0].status; // 'pending', 'accepted', or 'rejected'
  } catch (error) {
    console.error('Error getting follow status:', error);
    return null;
  }
};

// ==================== BLOCKS ====================

const blockUser = async (blockerId, blockedUsername) => {
  try {
    // Get blocked user's ID
    const userResult = await query(
      'SELECT id FROM users WHERE username = $1',
      [blockedUsername]
    );
    
    if (userResult.rows.length === 0) {
      return { success: false, message: 'User not found' };
    }
    
    const blockedId = userResult.rows[0].id;
    
    // Insert block record
    await query(
      'INSERT INTO user_blocks (blocker_id, blocked_id) VALUES ($1, $2) ON CONFLICT DO NOTHING',
      [blockerId, blockedId]
    );
    
    return { success: true, message: 'User blocked successfully' };
  } catch (error) {
    console.error('Error blocking user:', error);
    return { success: false, message: 'Error blocking user' };
  }
};

const blockUserById = async (blockerId, blockedId) => {
  try {
    // Verify user exists
    const userResult = await query(
      'SELECT id FROM users WHERE id = $1',
      [blockedId]
    );
    
    if (userResult.rows.length === 0) {
      return { success: false, message: 'User not found' };
    }
    
    // Insert block record
    await query(
      'INSERT INTO user_blocks (blocker_id, blocked_id) VALUES ($1, $2) ON CONFLICT DO NOTHING',
      [blockerId, blockedId]
    );
    
    return { success: true, message: 'User blocked successfully' };
  } catch (error) {
    console.error('Error blocking user by ID:', error);
    return { success: false, message: 'Error blocking user' };
  }
};

const unblockUser = async (blockerId, blockedUsername) => {
  try {
    const userResult = await query(
      'SELECT id FROM users WHERE username = $1',
      [blockedUsername]
    );
    
    if (userResult.rows.length === 0) {
      return { success: false, message: 'User not found' };
    }
    
    const blockedId = userResult.rows[0].id;
    
    await query(
      'DELETE FROM user_blocks WHERE blocker_id = $1 AND blocked_id = $2',
      [blockerId, blockedId]
    );
    
    return { success: true, message: 'User unblocked successfully' };
  } catch (error) {
    console.error('Error unblocking user:', error);
    return { success: false, message: 'Error unblocking user' };
  }
};

const isBlockedBy = async (blockerId, potentiallyBlockedId) => {
  try {
    const result = await query(
      'SELECT * FROM user_blocks WHERE blocker_id = $1 AND blocked_id = $2',
      [blockerId, potentiallyBlockedId]
    );
    return result.rows.length > 0;
  } catch (error) {
    console.error('Error checking block status:', error);
    return false;
  }
};

const getBlockedUsers = async (userId) => {
  try {
    const result = await query(
      `SELECT u.id, u.username FROM user_blocks ub
       JOIN users u ON ub.blocked_id = u.id
       WHERE ub.blocker_id = $1`,
      [userId]
    );
    return result.rows;
  } catch (error) {
    console.error('Error getting blocked users:', error);
    return [];
  }
};

// ==================== AVATAR ====================

const updateAvatar = async (userId, avatarUrl) => {
  try {
    const result = await query(
      `UPDATE users SET avatar = $1, updated_at = CURRENT_TIMESTAMP
       WHERE id = $2
       RETURNING id, username, avatar`,
      [avatarUrl, userId]
    );
    return result.rows[0];
  } catch (error) {
    console.error('Error updating avatar:', error);
    return null;
  }
};

const deleteAvatar = async (userId) => {
  try {
    const result = await query(
      `UPDATE users SET avatar = NULL, updated_at = CURRENT_TIMESTAMP
       WHERE id = $1
       RETURNING id, username, avatar`,
      [userId]
    );
    return result.rows[0];
  } catch (error) {
    console.error('Error deleting avatar:', error);
    return null;
  }
};

// ==================== BACKGROUND ====================

const updateBackground = async (userId, backgroundUrl) => {
  try {
    const result = await query(
      `UPDATE users SET background_image = $1, updated_at = CURRENT_TIMESTAMP
       WHERE id = $2
       RETURNING id, username, background_image`,
      [backgroundUrl, userId]
    );
    return result.rows[0];
  } catch (error) {
    console.error('Error updating background:', error);
    return null;
  }
};

// ==================== PRIVACY SETTINGS ====================

const getPrivacySettings = async (userId) => {
  try {
    const result = await query(
      'SELECT allow_private_chat, profile_privacy, allow_share_location FROM users WHERE id = $1',
      [userId]
    );
    if (result.rows.length === 0) {
      return { allowPrivateChat: 'everyone', profilePrivacy: 'everyone', allowShareLocation: false };
    }
    return { 
      allowPrivateChat: result.rows[0].allow_private_chat || 'everyone',
      profilePrivacy: result.rows[0].profile_privacy || 'everyone',
      allowShareLocation: result.rows[0].allow_share_location || false
    };
  } catch (error) {
    console.error('Error getting privacy settings:', error);
    return { allowPrivateChat: 'everyone', profilePrivacy: 'everyone', allowShareLocation: false };
  }
};

const updatePrivacySettings = async (userId, settings) => {
  try {
    const { allowPrivateChat, profilePrivacy, allowShareLocation } = settings;
    const validChatOptions = ['everyone', 'only_friends'];
    const validProfileOptions = ['everyone', 'only_friends', 'only_me'];
    
    const updates = [];
    const values = [];
    let paramIndex = 1;
    
    if (allowPrivateChat !== undefined) {
      const chatValue = validChatOptions.includes(allowPrivateChat) ? allowPrivateChat : 'everyone';
      updates.push(`allow_private_chat = $${paramIndex++}`);
      values.push(chatValue);
    }
    
    if (profilePrivacy !== undefined) {
      const profileValue = validProfileOptions.includes(profilePrivacy) ? profilePrivacy : 'everyone';
      updates.push(`profile_privacy = $${paramIndex++}`);
      values.push(profileValue);
    }

    if (allowShareLocation !== undefined) {
      updates.push(`allow_share_location = $${paramIndex++}`);
      values.push(allowShareLocation);
    }
    
    if (updates.length === 0) {
      return { success: false, error: 'No valid settings to update' };
    }
    
    updates.push('updated_at = CURRENT_TIMESTAMP');
    values.push(userId);
    
    await query(
      `UPDATE users SET ${updates.join(', ')} WHERE id = $${paramIndex}`,
      values
    );
    
    return { success: true };
  } catch (error) {
    console.error('Error updating privacy settings:', error);
    return { success: false, error: 'Failed to update' };
  }
};

const canSendPrivateMessage = async (senderId, recipientId) => {
  try {
    const settings = await getPrivacySettings(recipientId);
    
    if (settings.allowPrivateChat === 'everyone') {
      return { allowed: true };
    }
    
    if (settings.allowPrivateChat === 'only_friends') {
      const isFriend = await isFollowing(recipientId, senderId);
      const isMutual = await isFollowing(senderId, recipientId);
      
      if (isFriend || isMutual) {
        return { allowed: true };
      }
      return { allowed: false, reason: 'User only accepts private messages from friends' };
    }
    
    return { allowed: true };
  } catch (error) {
    console.error('Error checking PM permission:', error);
    return { allowed: true };
  }
};

module.exports = {
  // Posts
  createPost,
  getUserPosts,
  getPostCount,
  deletePost,
  
  // Gifts
  sendGift,
  getReceivedGifts,
  getSentGifts,
  getGiftCount,
  
  // Footprints
  getFootprintCount,
  
  // Follows (with pending/accepted system)
  followUser,
  unfollowUser,
  getFollowers,
  getFollowing,
  getFollowersCount,
  getFollowingCount,
  isFollowing,
  getFollowStatus,
  acceptFollowRequest,
  rejectFollowRequest,
  getPendingFollowRequests,
  getPendingFollowRequestsCount,
  
  // Blocks
  blockUser,
  blockUserById,
  unblockUser,
  isBlockedBy,
  getBlockedUsers,
  
  // Avatar & Background
  updateAvatar,
  deleteAvatar,
  updateBackground,
  
  // Privacy
  getPrivacySettings,
  updatePrivacySettings,
  canSendPrivateMessage
};
